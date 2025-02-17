# Databricks notebook source
# importing required libaries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import avg,count,desc,length,when,col
from pyspark.sql.functions import *


# COMMAND ----------

spark

# COMMAND ----------

cars_schema = StructType([
    StructField("new&used",StringType(),True),
    StructField("name",StringType(),True),
    StructField("money",IntegerType(),True),
    StructField("Exterior color",StringType(),True),
    StructField("Interior color",StringType(),True),
    StructField("Drivetrain",StringType(),True),
    StructField("MPG",StringType(),True),
    StructField("Fuel type",StringType(),True),
    StructField("Transmission",StringType(),True),
    StructField("Engine",StringType(),True),
    StructField("Mileage",IntegerType(),True),
    StructField("Convenience",StringType(),True),
    StructField("Entertainment",StringType(),True),
     StructField("Exterior",StringType(),True),
      StructField("Safety",StringType(),True),
       StructField("Seating",StringType(),True),
        StructField("Accidents or damage",StringType(),True),
         StructField("Clean title",BooleanType(),True),
          StructField("1-owner vehicle",BooleanType(),True),
           StructField("Personal use only",BooleanType(),True),
            StructField("brand",StringType(),True),
             StructField("Year",DateType(),True),
              StructField("Model",StringType(),True),
               StructField("currency",StringType(),True)
              
])

# COMMAND ----------

#creating my df by new_york_cars.csv
cars_df = spark.read.schema(cars_schema).format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/rohitjangid7887@gmail.com/New_York_cars.csv")


# COMMAND ----------

cars_df.display(5)

# COMMAND ----------

cars_df.describe().display()

# COMMAND ----------

#handling the missing data
cars_df.select([count(when(col(c).isNull(), c)).alias(c) for c in cars_df.columns]).display()

# COMMAND ----------

#drop the rows that is missing 
cars_df_clean=cars_df.dropna()


# COMMAND ----------

#filling specific values to the nan columns or missing columns
cars_df_filled=cars_df.fillna({"Exterior color":"black","Interior color":"green","Personal use only":"No","Accidents or damage":'None Reported'})

# COMMAND ----------

cars_df_filled.display(5)

# COMMAND ----------

high_rated_cars=cars_df_filled.filter((col("Mileage")>302))
high_rated_cars.display(5)


# COMMAND ----------

# group by the model and calculet the money
avg_money_by_car_model=cars_df_filled.groupBy("Model").avg("money")
avg_money_by_car_model.display()

# COMMAND ----------

#Sort cars by a specific column price in descending order
sorted_df = cars_df.sort(cars_df['money'].desc())
sorted_df.display()

# COMMAND ----------

# Select specific columns from the DataFrame.

selected_df = cars_df.select('brand', 'model', 'money')
selected_df.display(5)

# COMMAND ----------

# Add a new column to the DataFramea column indicating if the car is expensive
cars_df = cars_df.withColumn('is_expensive', cars_df['money'] > 30000)
cars_df.display(5)

# COMMAND ----------

#Total  Revenue by model of the cars

total_revenue_by_model=cars_df_filled.groupBy("Model").agg(sum("money"))
total_revenue_by_model.display()

# COMMAND ----------

cars_df.write.csv("to_save_file.csv", header=True)

# COMMAND ----------


