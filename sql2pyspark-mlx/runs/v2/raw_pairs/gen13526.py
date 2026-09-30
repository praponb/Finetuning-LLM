from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") > 62)
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("age")).otherwise(0)).alias("singapore_age"),
    )
    .filter(F.col("singapore_age") > 71)
    .select("is_active", "singapore_age")
    .orderBy("singapore_age", "is_active")
    .limit(10)
)
