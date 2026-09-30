from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location", "dept_name")
    .agg(
        F.min("budget").alias("min_budget"),
        F.count(F.when(F.col("location") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("min_budget") > 1328000)
)
