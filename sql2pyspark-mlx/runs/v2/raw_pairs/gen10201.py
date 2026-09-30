from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Bangkok")
    .groupBy("dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("min_budget") >= 1253000)
    .orderBy("min_budget", "dept_name")
)
