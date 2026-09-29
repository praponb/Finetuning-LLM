from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([3, 5, 7, 8]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("inventory_count"),
    )
    .filter(F.col("inventory_count") < 5)
)
