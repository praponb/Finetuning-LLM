from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "right")
    .join(df_stores.alias("s"), F.col("i.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.cost") > 580)
    .groupBy(F.month(F.col("p.launch_date")).alias("launch_month"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy("latest_last_restock", F.col("launch_month").desc())
)
