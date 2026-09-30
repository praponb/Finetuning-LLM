from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel", "status")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.round(F.stddev("discount"), 2).alias("stddev_discount"),
        (F.max("discount") - F.min("discount")).alias("discount_range"),
    )
    .filter(F.col("discount_range") >= 32)
    .orderBy(F.col("channel").desc(), "stddev_discount")
)
