from pyspark.sql import functions as F

result = (
    df_inventory
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.id", "src.store_id", "src.last_restock", F.col("p.store_name"))
)
