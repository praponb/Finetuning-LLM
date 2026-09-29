from pyspark.sql import functions as F

result = (
    df_inventory.alias("inventory")
    .join(df_stores.filter(F.col("sqft") <= 15688).alias("stores"), F.col("inventory.store_id") == F.col("stores.id"), "left_semi")
    .filter(F.col("inventory.quantity").between(66, 151))
    .select(F.col("inventory.id"), F.col("inventory.store_id"))
    .orderBy(F.col("store_id").desc(), "id")
    .limit(10)
)
