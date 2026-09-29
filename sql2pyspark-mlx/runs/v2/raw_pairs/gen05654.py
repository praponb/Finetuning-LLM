from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("o.customer_id").isin([27, 29]))
    .groupBy(F.col("s.id"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
    )
    .orderBy(F.col("avg_shipping_fee").desc(), "id")
    .limit(5)
)
