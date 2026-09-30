from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("o.discount") >= 0.13) & (F.col("s.city") != "Tokyo"))
    .select(
        F.col("o.discount"),
        F.col("c.is_active"),
        F.col("s.region"),
        (F.col("o.discount") * F.col("o.shipping_fee")).alias("discount_times_shipping_fee"),
    )
    .orderBy("discount_times_shipping_fee")
)
