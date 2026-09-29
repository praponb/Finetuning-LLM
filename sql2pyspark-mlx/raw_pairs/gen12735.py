from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .filter(F.col("stores.city") == "Bangkok")
    .groupBy(
        F.when(F.col("stores.sqft") >= 14449, "high").when(F.col("stores.sqft") >= 6079, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.count("*").alias("inventory_count"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .orderBy(F.col("inventory_count").desc())
)
