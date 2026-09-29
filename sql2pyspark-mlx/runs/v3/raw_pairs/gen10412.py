from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2]))
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

summary = (
    filtered_inventory
    .groupBy("product_id", "store_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = summary
