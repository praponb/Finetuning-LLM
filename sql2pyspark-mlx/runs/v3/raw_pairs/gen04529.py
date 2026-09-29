from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .filter((F.col("s.region") != "South") | (F.col("s.sqft") != 8656))
    .select(
        F.col("ss.revenue"),
        F.col("ss.store_id"),
        F.col("p.product_name"),
        F.col("p.cost"),
        F.col("s.region"),
    )
    .orderBy("region")
)
