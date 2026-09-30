from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(F.count(F.when(F.col("location") == "London", 1)).alias("london_count"))
    .filter(F.col("london_count") >= 5)
)
