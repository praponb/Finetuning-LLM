from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter((F.length(F.col("p.product_name")) > 11) & (F.col("p.cost") <= 300))
    .select(
        F.col("s.city"),
        F.col("s.supplier_name"),
        F.col("p.brand"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
    .orderBy("price", F.col("brand").desc())
)
