from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("p.product_name").like("R%") & (F.col("s.region") == "South"))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("s.city"),
        F.col("s.region"),
        F.col("p.price"),
        F.col("p.stock_qty"),
    )
    .orderBy(F.col("city").desc())
)
