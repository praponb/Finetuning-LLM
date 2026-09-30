from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .filter((F.col("o.status") == "SHIPPED") | (F.col("p.brand") == "Initech"))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.product_id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("p.price"),
        F.substring(F.col("p.product_name"), 1, 3).alias("product_name_prefix"),
    )
)
