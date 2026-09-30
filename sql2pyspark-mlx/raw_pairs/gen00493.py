from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter((F.col("ss.revenue") >= 3900) | (F.col("p.cost") <= 400))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.store_id"),
        F.col("s.store_name"),
        F.col("p.stock_qty"),
        F.col("p.category"),
    )
    .orderBy("store_id", F.col("stock_qty").desc(), "id")
    .limit(3)
)
