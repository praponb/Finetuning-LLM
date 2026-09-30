from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("i.store_id"), "left")
    .filter(F.col("s.sqft") < 12786)
    .groupBy(F.col("s.region"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
)
