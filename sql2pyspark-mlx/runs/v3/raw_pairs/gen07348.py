from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("s.sqft") > 8315)
    .select(
        F.col("ss.id"),
        F.col("ss.product_id"),
        F.col("s.city"),
        F.col("p.product_name"),
    )
)
