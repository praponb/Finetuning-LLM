from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("stores.city") == "London")
    .select(
        F.col("inventory.last_restock"),
        F.col("stores.city"),
        F.col("stores.opened_date"),
        F.when(F.col("inventory.quantity") >= 209, "High").when(F.col("inventory.quantity") >= 65, "Medium").otherwise("Low").alias("quantity_level"),
    )
)
