from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "outer")
    .filter((F.col("p.price") < 350) | F.col("ss.product_id").isin([1, 5, 7]))
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("p.stock_qty"),
        F.col("p.category"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.trim(F.col("s.city")).alias("clean_city"),
    )
    .orderBy("id", F.col("city").desc())
)
