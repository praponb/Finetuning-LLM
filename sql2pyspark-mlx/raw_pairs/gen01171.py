from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.avg("budget").alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
)
