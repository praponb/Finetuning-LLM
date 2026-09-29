from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "right")
    .filter(F.col("stores.sqft") > 4436)
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("stores.opened_date"),
        F.col("stores.sqft"),
    )
    .orderBy("opened_date", F.col("sqft").desc())
)
