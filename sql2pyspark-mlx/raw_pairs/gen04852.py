from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.filter(F.month("launch_date").isin([3, 6, 8])).alias("p"), F.col("i.product_id") == F.col("p.id"), "left_anti")
    .select(F.col("i.id"), F.col("i.last_restock"))
)
