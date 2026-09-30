from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("s.city") == "London")
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.city"),
        F.col("s.store_name"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.round(F.col("p.price"), 1).alias("price_rounded"),
    )
)
