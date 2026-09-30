from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .select(
        F.col("ss.store_id"),
        F.col("ss.product_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
    )
    .orderBy("product_id")
)
