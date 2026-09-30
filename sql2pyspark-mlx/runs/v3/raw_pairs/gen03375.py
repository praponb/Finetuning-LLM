from pyspark.sql import functions as F

result = (
    df_stores.alias("stores")
    .join(df_inventory.filter(F.col("product_id").isin([1, 4, 6, 10])).alias("inventory"), F.col("stores.id") == F.col("inventory.store_id"), "left_anti")
    .select(F.col("stores.id"), F.col("stores.city"), F.col("stores.region"))
    .orderBy("city", "id")
    .limit(10)
)
