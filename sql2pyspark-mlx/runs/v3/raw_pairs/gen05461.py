from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("sqft") >= 10183).alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("inventory.quantity").between(166, 175))
    .select(
        F.col("inventory.id"),
        F.col("inventory.product_id"),
        F.col("inventory.quantity"),
        F.col("inventory.store_id"),
    )
    .orderBy("id", F.col("quantity").desc())
)
