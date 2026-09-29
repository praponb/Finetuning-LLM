from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.sale_date"),
        F.col("p.supplier_id"),
        F.col("p.category"),
        F.col("s.store_name"),
    )
    .orderBy(F.col("category").desc(), F.col("store_name").desc())
)
