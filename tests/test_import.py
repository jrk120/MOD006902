#Import necessary modules
import pytest
import os
from pyspark.testing.utils import assertSchemaEqual
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, date_format, col

#---------------------------------------------pre define repeated parameters for test------------------------------------

@pytest.fixture
def spark_fixture():
    spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
    yield spark

@pytest.fixture
def expectedSchema(spark_fixture):
    expectedSchema = StructType([
        StructField("gameId", LongType(), True),
        StructField("playId", LongType(), True),
        StructField("nflId", DoubleType(), True),
        StructField("frameId", LongType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("frameType", StringType(), True)
    ])
    return expectedSchema

@pytest.fixture
def trackingDf(spark_fixture, expectedSchema):
    folder = r"C:\Users\knigh\MOD006902\data\tracking"
    files = [os.path.join(folder, f) for f in os.listdir(folder) if f.endswith(".parquet")]
    return spark_fixture.read.schema(expectedSchema).parquet(*files)


#------------------------------------------------------Test functions----------------------------------------------------------
def test_tracking_import_schema(spark_fixture, expectedSchema, trackingDf):
    '''
    This function checks if there are missing columns or schema drift for future reuse
    '''
    assertSchemaEqual(trackingDf.schema, expectedSchema, ignoreNullable=True)


def test_if_columns_have_nulls(spark_fixture, expectedSchema, trackingDf):
    '''
    This function checks to see if any of the columns are null
    '''
    expectedColumns = [field.name for field in expectedSchema.fields]
    nulls = [column for column in expectedColumns if trackingDf.select(column).distinct().count() == 1 and trackingDf.select(column).first()[0] is None]
    print(f"Null columnns: {nulls}")
    assert not nulls

def test_parquet_conversion(spark_fixture):
    truthCSVs = r"C:\Users\knigh\MOD006902\tests\test_data"
    convertedData = r"C:\Users\knigh\MOD006902\data"

    #Files to test
    files = ["games"]

    csvs = {}
    parqeuts = {}
    for file in files:
        csvs[file] = spark_fixture.read.option("header", True).option("inferSchema", True).csv(truthCSVs + "/" + file + ".csv")
        parqeuts[file] = spark_fixture.read.parquet(convertedData + "/" + file + ".parquet")

    for file, CSVDf in csvs.items():

        parqeutDf = parqeuts[file]

        #Avoiding nuance with how it is read in, some start with 0s e.g 09:30:00 while the same data in the other is 9:30:00
        if file == "games":
            parqeutDf = parqeutDf.withColumn("gameTimeEastern", date_format(to_timestamp(col("gameTimeEastern"), "H:mm:ss"), "HH:mm:ss"))
            CSVDf = CSVDf.withColumn("gameTimeEastern", date_format(to_timestamp(col("gameTimeEastern"), "H:mm:ss"), "HH:mm:ss"))

        csvRows = set([tuple(row) for row in CSVDf.collect()])
        parqeutRows = set([tuple(row) for row in parqeutDf.collect()])

        #Did not deploy assertDataFrameEqual
        #Only wanted to compare the actual values, schemas dont have to match due to parquet's perception of type
        assert csvRows == parqeutRows, "DataFrames do not match"
    

    
