from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("s.store_name"),
    )
    .orderBy("stock_qty", "id")
    .limit(1)
)
