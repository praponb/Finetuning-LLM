from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("p.stock_qty"),
        F.col("s.region"),
    )
    .orderBy(F.col("revenue").desc())
)
