from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        F.max("budget").alias("max_budget"),
        F.sum(F.when(F.col("dept_name") == "Engineering", 1).otherwise(0)).alias("engineering_count"),
    )
    .orderBy(F.col("min_budget").desc())
)
