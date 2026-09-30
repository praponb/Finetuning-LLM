from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("store_sales.store_id") == F.col("stores.id"), "right")
    .groupBy(
        F.when(F.col("stores.sqft") >= 16609, "high").when(F.col("stores.sqft") >= 6669, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.min(F.col("store_sales.revenue")).alias("min_revenue"))
    .orderBy(F.col("min_revenue").desc())
)
