from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .filter(F.col("p.launch_date").between("2023-03-01", "2024-01-01") & (F.col("ss.units") <= 39))
    .select(
        F.col("ss.units"),
        F.col("s.sqft"),
        F.col("s.city"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
    )
)
