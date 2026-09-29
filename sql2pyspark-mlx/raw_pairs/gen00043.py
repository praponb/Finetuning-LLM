from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter((F.col("ss.revenue") <= 3300) & (F.col("s.city") != "Chiang Mai"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.sale_date"),
        F.col("s.city"),
        F.col("p.brand"),
    )
    .orderBy("sale_date")
)
