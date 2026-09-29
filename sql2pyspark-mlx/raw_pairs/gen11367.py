from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("age")).otherwise(0)).alias("corporate_age"),
        F.count("credit_limit").alias("credit_limit_count"),
    )
    .orderBy(F.col("corporate_age").desc())
)
