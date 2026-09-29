from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "left")
    .filter(F.col("p.category").isin(["Sports", "Home"]))
    .groupBy(F.col("p.brand"), F.col("p.category"))
    .agg(F.min(F.col("i.last_restock")).alias("min_last_restock"))
    .orderBy("category", "min_last_restock")
)
