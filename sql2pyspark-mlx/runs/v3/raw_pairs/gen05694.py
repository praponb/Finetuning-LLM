from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("location")
    .agg(
        F.sum(F.when(F.col("location") == "Singapore", F.col("budget")).otherwise(0)).alias("singapore_budget"),
    )
    .filter(F.col("singapore_budget") >= 48)
    .select("location", "singapore_budget")
    .orderBy("location")
)
