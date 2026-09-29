from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name", "location")
    .agg(F.count("budget").alias("budget_count"))
    .orderBy("budget_count", F.col("location").desc(), "dept_name")
    .limit(1)
)
