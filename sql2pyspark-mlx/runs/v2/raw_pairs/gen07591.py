from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.round(F.avg("budget"), 2).alias("avg_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .orderBy("location")
)
