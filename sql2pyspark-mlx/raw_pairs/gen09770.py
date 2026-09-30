from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("ss.product_id"),
        F.col("ss.revenue"),
        F.col("s.opened_date"),
        F.col("s.city"),
        F.col("s.sqft"),
        F.when(F.col("s.sqft") >= 12663, "High").when(F.col("s.sqft") >= 2785, "Medium").otherwise("Low").alias("sqft_tier"),
    )
)
