from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_products.alias("p"), F.col("oi.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .select(
        F.col("oi.product_id"),
        F.col("p.cost"),
        F.col("s.city"),
        F.col("s.country"),
    )
    .orderBy("city", F.col("product_id").desc())
)
