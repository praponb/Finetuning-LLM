from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "CANCELLED"]) & (F.col("shipping_fee") >= 38))
    .groupBy("store_id")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.round(((100 * F.sum(F.when(F.col("status") == "SHIPPED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_shipped"),
    )
    .orderBy("pct_shipped", "store_id")
    .limit(10)
)
