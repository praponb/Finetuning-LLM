from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category"), F.col("p.brand"))
    .agg(F.max(F.col("i.last_restock")).alias("latest_last_restock"))
    .orderBy("latest_last_restock")
)
