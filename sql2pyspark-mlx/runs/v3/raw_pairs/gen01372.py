from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left")
    .select(
        F.col("stores.store_name"),
        F.col("stores.sqft"),
        F.col("stores.opened_date"),
        F.col("inventory.quantity"),
    )
    .orderBy(F.col("quantity").desc(), "opened_date")
)
