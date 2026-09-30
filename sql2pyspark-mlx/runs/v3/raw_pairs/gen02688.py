from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_products.alias("products"), F.col("inventory.product_id") == F.col("products.id"), "left")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("inventory.store_id").isin([2, 3]))
    .groupBy(
        F.col("stores.city"),
        F.when(F.col("stores.sqft") >= 13278, "high").when(F.col("stores.sqft") >= 6181, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        F.count("*").alias("row_count"),
    )
    .orderBy("city")
)
