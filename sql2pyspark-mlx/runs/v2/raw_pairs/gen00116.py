from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("o.store_id").isin([11, 25, 37]))
    .select(
        F.col("s.order_id"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("c.is_active"),
        F.coalesce(F.col("c.city"), F.lit("N/A")).alias("city_filled"),
    )
    .orderBy("order_id")
)
