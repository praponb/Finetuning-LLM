from pyspark.sql import functions as F

src = (
    df_departments
    .filter(F.col("budget").between(1280000, 3784000) & (F.col("location") != "Singapore"))
)

summary = (
    src
    .groupBy("location")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.avg("budget").alias("avg_budget"),
    )
)

result = summary.orderBy(F.col("budget_range").desc(), F.col("avg_budget").desc())
