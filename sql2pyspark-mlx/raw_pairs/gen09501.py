from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.sum("shipping_fee").alias("total_shipping_fee"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .orderBy(F.col("store_count").desc(), F.col("total_shipping_fee").desc())
)
