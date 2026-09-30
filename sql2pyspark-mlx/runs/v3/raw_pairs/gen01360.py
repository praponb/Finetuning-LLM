from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .groupBy(
        F.when(F.col("stores.city").isin(["Bangkok", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.sum(F.col("inventory.quantity")).alias("total_quantity"),
        F.avg(F.col("inventory.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc(), "city_group")
    .limit(1)
)
