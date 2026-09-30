from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.category") == "Fashion") & F.col("p.product_name").like("%Hub%"))
    .select(
        F.col("oi.id"),
        F.col("oi.product_id"),
        F.col("oi.quantity"),
        F.col("p.launch_date"),
        F.col("p.supplier_id"),
        F.col("o.discount"),
        F.col("o.amount"),
    )
    .orderBy(F.col("product_id").desc(), "id")
    .limit(1)
)
