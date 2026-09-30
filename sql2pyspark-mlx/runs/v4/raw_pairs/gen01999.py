from pyspark.sql import functions as F

recent_inventory = df_inventory.filter(F.col("product_id").isin([3, 4, 9, 10]))

agg = (
    recent_inventory
    .groupBy("product_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = (
    agg
    .select("product_id", "avg_quantity")
    .orderBy("avg_quantity")
)
