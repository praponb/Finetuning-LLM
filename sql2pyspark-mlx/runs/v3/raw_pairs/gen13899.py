from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("inventory_count") < 2)
    .drop("inventory_count")
)
