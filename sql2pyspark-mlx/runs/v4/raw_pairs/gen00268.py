from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.month("last_restock").isin([4, 9, 12]))
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") > 120)
)
