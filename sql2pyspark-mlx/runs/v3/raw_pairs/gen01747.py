from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.is_active"))
    .agg(
        F.avg(F.col("o.discount")).alias("avg_discount"),
        F.round(F.avg(F.col("o.shipping_fee")), 2).alias("avg_shipping_fee"),
    )
)
