from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.sqft") <= 4704)
    .groupBy(F.col("stores.city"))
    .agg(
        F.count("*").alias("total_count"),
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
    )
    .filter(F.col("total_count") >= 2)
)
