from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy(
        F.when(F.col("budget") >= 3964000, "high").when(F.col("budget") >= 1864000, "mid").otherwise("low").alias("budget_bucket"),
    )
    .agg(
        F.max("budget").alias("max_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") >= 51)
    .orderBy(F.col("budget_range").desc())
)
