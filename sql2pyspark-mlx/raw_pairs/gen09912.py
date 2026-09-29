from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("city").isin(["Berlin", "New York", "Tokyo"])).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.last_restock"))
    .orderBy("last_restock", F.col("id").desc())
)
