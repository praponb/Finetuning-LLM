from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "inner")
    .filter((F.length(F.col("s.supplier_name")) < 11) & F.col("p.price").isNotNull())
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.category"),
        F.col("p.price"),
        F.col("s.city"),
    )
    .orderBy("category", F.col("store_id").desc())
)
