from pyspark.sql import functions as F

recent_inventory = (
    df_inventory
    .filter((F.col("quantity") <= 132) & (F.col("last_restock") <= "2023-06-17"))
)

agg = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = agg
