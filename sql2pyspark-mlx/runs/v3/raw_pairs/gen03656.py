from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name") != "Legal")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.avg("budget").alias("avg_budget"),
    )
)
