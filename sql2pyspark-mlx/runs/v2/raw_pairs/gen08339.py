from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~(F.col("shipping_fee").between(27, 40) & ((F.col("shipping_fee") < 14) | F.col("channel").isin(["mobile", "web"]))))
    .groupBy("store_id", "status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
        F.max("shipping_fee").alias("max_shipping_fee"),
    )
    .orderBy(F.col("max_shipping_fee").desc(), F.col("returned_count").desc())
)
