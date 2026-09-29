from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("stores.id") == F.col("inventory.store_id"), "inner")
    .groupBy(F.col("stores.region"))
    .agg(
        (F.max(F.col("inventory.quantity")) - F.min(F.col("inventory.quantity"))).alias("quantity_range"),
        F.max(F.col("inventory.quantity")).alias("max_quantity"),
    )
    .orderBy(F.col("quantity_range").desc())
)
