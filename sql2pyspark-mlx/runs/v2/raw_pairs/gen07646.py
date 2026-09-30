from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("p.price"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
    )
    .orderBy(F.col("store_id").desc(), F.col("price").desc())
)
