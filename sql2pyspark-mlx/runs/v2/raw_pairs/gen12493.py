from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter((F.col("s.sqft") < 5591) | (F.col("o.shipping_fee") > 36))
    .select(
        F.col("oi.quantity"),
        F.col("oi.unit_price"),
        F.col("o.customer_id"),
        F.col("s.opened_date"),
    )
)
