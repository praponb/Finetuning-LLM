from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.when(F.col("stores.sqft") >= 16708, "high").when(F.col("stores.sqft") >= 6601, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(F.max(F.col("inventory.last_restock")).alias("latest_last_restock"))
    .orderBy(F.col("sqft_bucket").desc(), "latest_last_restock")
    .limit(10)
)
