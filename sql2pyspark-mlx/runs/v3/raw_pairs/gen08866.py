from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.37, "high").when(F.col("discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
    .orderBy(F.col("pct_partner").desc())
)
