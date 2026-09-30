from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Singapore", "London"]) | F.col("budget").between(1366000, 1446000))
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.count(F.when(F.col("location") == "Berlin", 1)).alias("berlin_count"),
        F.sum(F.when(F.col("dept_name") == "HR", 1).otherwise(0)).alias("hr_count"),
    )
)
