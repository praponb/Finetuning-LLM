from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(F.round(F.stddev(F.col("o.shipping_fee")), 2).alias("stddev_shipping_fee"))
    .filter(F.col("stddev_shipping_fee") >= 48)
    .orderBy("region")
)
