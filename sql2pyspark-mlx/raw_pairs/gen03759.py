from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("p.category"),
    )
    .orderBy(F.col("product_id").desc())
)
