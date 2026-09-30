from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("s.sqft") >= 5860)
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.store_name"),
        F.col("s.sqft"),
        F.col("p.brand"),
    )
)
