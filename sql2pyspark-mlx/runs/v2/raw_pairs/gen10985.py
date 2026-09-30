from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("p.id") == F.col("oi.product_id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .filter((F.col("o.shipping_fee") >= 23) | (F.col("o.order_date") >= "2024-05-01"))
    .select(
        F.col("oi.id"),
        F.col("oi.quantity"),
        F.col("p.category"),
        F.col("p.supplier_id"),
        F.col("o.amount"),
        F.floor(F.col("oi.quantity")).alias("quantity_whole"),
    )
    .orderBy("category", "quantity")
)
