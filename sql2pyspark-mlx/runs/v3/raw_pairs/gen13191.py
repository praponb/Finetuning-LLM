from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") <= 1020)
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.count("*").alias("order_count"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
    )
    .orderBy(F.col("shipping_fee_range").desc())
)
