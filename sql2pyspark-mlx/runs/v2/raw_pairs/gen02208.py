from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget").between(2925000, 3763000))
    .agg(
        F.sum("budget").alias("total_budget"),
        F.count("budget").alias("budget_count"),
    )
    .orderBy(F.col("budget_count").desc())
)
