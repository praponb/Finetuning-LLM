from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "outer")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("ss.id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("p.supplier_id"),
        F.col("p.price"),
        F.col("s.region"),
        F.col("s.city"),
        F.upper(F.col("s.store_name")).alias("store_name_upper"),
    )
    .orderBy(F.col("product_id").desc())
)
