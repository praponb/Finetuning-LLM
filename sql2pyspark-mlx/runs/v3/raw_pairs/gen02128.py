from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2522000, 2980000))
    .groupBy("dept_name")
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 5)
    .orderBy("total_count")
)
