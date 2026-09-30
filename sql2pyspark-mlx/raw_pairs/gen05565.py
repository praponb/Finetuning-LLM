from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") > "2025-10-06")
    .select(
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("o.channel"),
        F.col("o.status"),
    )
    .orderBy("order_id")
)
