from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.product_id"),
        F.col("ss.store_id"),
        F.col("s.city"),
        F.col("p.brand"),
        F.length(F.col("s.region")).alias("region_length"),
    )
)
