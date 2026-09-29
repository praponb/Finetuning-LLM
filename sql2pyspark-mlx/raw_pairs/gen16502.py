from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & (F.col("dept_name") != "Legal"))
    .groupBy("location")
    .agg(F.count("*").alias("cnt"))
)
