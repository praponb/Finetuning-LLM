from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .groupBy(
        F.when(F.col("stores.region").isin(["West", "North"]), "tier_1").otherwise("other").alias("region_group"),
    )
    .agg(
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
    )
    .orderBy("max_quantity")
)
