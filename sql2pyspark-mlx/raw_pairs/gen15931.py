from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter(F.lower(F.col("p.product_name")).like("%ate%") & F.col("ss.product_id").isin([1, 3, 9]))
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("p.supplier_id"),
        F.col("p.product_name"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("product_id").desc(), "store_name")
)
