from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"),
        F.sum(F.when(F.col("o.channel") == "store", 1).otherwise(0)).alias("store_count"),
    )
    .filter(F.col("store_count") > 61)
)
