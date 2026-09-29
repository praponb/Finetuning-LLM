from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.col("quantity") > 238).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_semi")
    .select(F.col("stores.id"), F.col("stores.sqft"), F.col("stores.store_name"))
)
