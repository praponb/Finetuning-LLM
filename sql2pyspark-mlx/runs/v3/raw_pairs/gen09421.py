from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.amount") > 890)
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("p.price"),
        F.col("p.supplier_id"),
        F.col("o.channel"),
    )
    .orderBy("supplier_id", "id")
    .limit(10)
)
