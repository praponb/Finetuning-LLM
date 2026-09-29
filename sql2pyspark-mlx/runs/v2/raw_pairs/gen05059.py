from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.min("budget").alias("min_budget"),
        (F.max("budget") - F.min("budget")).alias("budget_range"),
    )
    .filter(F.col("budget_range") < 33)
    .orderBy("location")
)
