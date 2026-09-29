from pyspark.sql import functions as F

filtered_inventory = (
    df_inventory
    .filter(F.col("product_id").isin([4, 7, 8, 9]))
    .withColumn("adjusted_quantity", F.round((F.col("quantity") * 100), 2))
)

inventory_stats = (
    filtered_inventory
    .groupBy("product_id")
    .agg(F.round(F.stddev("adjusted_quantity"), 2).alias("stddev_adjusted_quantity"))
)

result = inventory_stats.orderBy(F.col("stddev_adjusted_quantity").desc())
