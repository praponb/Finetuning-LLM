from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "web"]) & (F.col("amount") > 1000) & F.col("store_id").isNull())
    .groupBy(
        F.when(F.col("shipping_fee") >= 31, "high").when(F.col("shipping_fee") >= 5, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "status",
    )
    .agg(
        F.sum(F.when(F.col("status") == "PENDING", F.col("discount")).otherwise(0)).alias("pending_discount"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("pending_discount").desc())
)
