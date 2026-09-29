from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("o.amount") <= 900) & F.col("o.channel").isNotNull())
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.shipping_fee"),
        F.col("p.cost"),
        F.col("p.category"),
        F.floor(F.col("oi.quantity")).alias("quantity_whole"),
    )
)
