from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "Finance"]) & (F.col("budget") < 1199000) & F.col("dept_name").isin(["HR", "Finance"]))
    .groupBy("dept_name")
    .agg(
        F.sum("budget").alias("total_budget"),
        F.min("budget").alias("min_budget"),
    )
    .filter(F.col("total_budget") >= 12709000)
)
