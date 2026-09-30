from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .filter(F.col("s.store_name").like("%Riversi%") | (F.col("c.age") != 55))
    .select(
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("c.is_active"),
        F.col("c.email"),
        F.col("s.region"),
        F.col("s.opened_date"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
