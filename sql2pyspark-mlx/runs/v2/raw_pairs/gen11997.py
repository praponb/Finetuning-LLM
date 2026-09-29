from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.channel").isNull())
    .groupBy(F.col("s.id"), F.col("s.region"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
    )
    .orderBy("region")
)
