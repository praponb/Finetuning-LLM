from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "left")
    .select(
        F.col("ss.id"),
        F.col("ss.store_id"),
        F.col("ss.revenue"),
        F.col("ss.product_id"),
        F.col("s.sqft"),
        F.round((F.col("s.sqft") * 0.85), 2).alias("adjusted_sqft"),
    )
)
