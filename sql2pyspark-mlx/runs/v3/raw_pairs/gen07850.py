from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee").isNotNull())
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.max(F.col("o.discount")).alias("max_discount"),
    )
    .filter(F.col("order_count") > 5)
    .orderBy("city")
)
