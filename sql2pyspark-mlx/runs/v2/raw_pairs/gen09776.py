from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("s.city"),
    )
    .orderBy(F.col("id").desc(), "product_id")
    .limit(5)
)
