from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(F.count("*").alias("total_count"))
    .orderBy(F.col("total_count").desc(), "dept_name")
)
