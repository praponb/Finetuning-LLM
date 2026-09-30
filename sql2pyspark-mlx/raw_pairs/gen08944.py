from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.count("budget").alias("budget_count"),
        F.max("budget").alias("max_budget"),
    )
    .filter(F.col("budget_count") > 4)
    .select("location", "budget_count", "max_budget")
    .orderBy("budget_count", F.col("max_budget").desc())
)
