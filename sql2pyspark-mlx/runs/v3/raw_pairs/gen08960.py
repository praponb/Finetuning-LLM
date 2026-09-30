from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left")
    .filter(F.col("stores.region").isNull())
    .groupBy(F.col("stores.region"), F.col("stores.city"))
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        F.min(F.col("inventory.quantity")).alias("min_quantity"),
    )
    .orderBy("region")
)
