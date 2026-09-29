from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_inventory.filter(F.col("last_restock") < "2025-10-01").alias("i"), F.col("p.id") == F.col("i.product_id"), "left_semi")
    .select(F.col("p.id"), F.col("p.stock_qty"), F.col("p.cost"))
)
