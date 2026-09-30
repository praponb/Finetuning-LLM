from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("store_id") == 5) & (F.col("last_restock") < "2023-04-01"))
    .select("id", "store_id", F.round("quantity", 2).alias("quantity_rounded"))
)
