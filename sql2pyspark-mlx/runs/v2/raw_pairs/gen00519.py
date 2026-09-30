from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_stores.filter(F.col("store_name").like("%Mall%")).alias("s"), F.col("i.store_id") == F.col("s.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.store_id"), F.col("i.product_id"))
    .orderBy("store_id", F.col("product_id").desc())
)
