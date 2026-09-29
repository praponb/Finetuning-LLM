from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter(F.col("ss.store_id").isin([1, 2, 4, 6]) & (F.col("s.city") == "Berlin"))
    .select(
        F.col("ss.sale_date"),
        F.col("ss.revenue"),
        F.col("p.supplier_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.array_contains(F.col("p.tags"), "premium").alias("is_premium"),
    )
)
