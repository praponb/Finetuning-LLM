from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("i.product_id") == F.col("p.id"), "left")
    .filter(F.col("i.last_restock") > "2024-11-03")
    .select(F.col("i.id"), F.col("i.store_id"), F.col("p.category"))
)
