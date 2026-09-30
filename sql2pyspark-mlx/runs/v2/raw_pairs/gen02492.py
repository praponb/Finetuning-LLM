from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 7)
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "CANCELLED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cancelled"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .orderBy("pct_cancelled")
)
