from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("oi.unit_price"),
        F.col("oi.order_id"),
        F.col("o.status"),
        F.col("o.store_id"),
        F.col("p.cost"),
        F.col("p.supplier_id"),
        F.trim(F.col("o.channel")).alias("clean_channel"),
    )
)
