from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("shipping_fee") >= 35, "high").when(F.col("shipping_fee") >= 12, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "store", 1).otherwise(0))) / F.count("*")), 1).alias("pct_store"),
    )
    .filter(F.col("pct_store") < 61)
    .select("status", "shipping_fee_bucket", "pct_store")
    .orderBy(F.col("shipping_fee_bucket").desc())
)
