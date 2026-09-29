from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .filter((F.col("p.cost") < 600) & (F.col("ss.units") >= 18))
    .select(
        F.col("ss.product_id"),
        F.col("p.stock_qty"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
