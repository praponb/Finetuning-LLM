from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.month("last_restock").isin([7, 8, 10])).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_semi")
    .select(
        F.col("stores.id"),
        F.col("stores.sqft"),
        F.col("stores.city"),
        F.col("stores.region"),
    )
    .orderBy("id", "region")
)
