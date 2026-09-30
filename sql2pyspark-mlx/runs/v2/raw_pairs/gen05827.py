from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("oi.quantity"),
        F.col("p.cost"),
        F.col("s.supplier_name"),
        F.col("s.country"),
    )
    .orderBy("supplier_name")
)
