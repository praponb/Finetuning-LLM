from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("store_sales.store_id"), "inner")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "left")
    .groupBy(
        F.when(F.col("stores.sqft") >= 16770, "high").when(F.col("stores.sqft") >= 5081, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.max(F.col("store_sales.sale_date")).alias("latest_sale_date"),
        F.count(F.col("store_sales.revenue")).alias("revenue_count"),
    )
    .orderBy("revenue_count", "sqft_bucket")
)
