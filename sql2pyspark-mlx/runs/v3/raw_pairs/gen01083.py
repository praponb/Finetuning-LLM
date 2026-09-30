from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.region").isNull())
    .select(
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("s.region"),
        F.col("p.category"),
        F.col("p.brand"),
    )
)
