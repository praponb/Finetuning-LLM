from pyspark.sql import functions as F

src = df_inventory.filter(F.col("quantity").between(65, 221) | (F.col("store_id") == 2))

summary = (
    src
    .groupBy("store_id")
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = summary.orderBy("avg_quantity")
