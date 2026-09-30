from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("p.launch_date"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.store_name"),
    )
)
