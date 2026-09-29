from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "right")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("oi.order_id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("s.supplier_name"),
    )
    .orderBy(F.col("price").desc(), F.col("order_id").desc())
)
