from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNotNull() | (F.col("dept_name") == "Engineering"))
    .withColumn("dept_name_length", F.length("dept_name"))
)
