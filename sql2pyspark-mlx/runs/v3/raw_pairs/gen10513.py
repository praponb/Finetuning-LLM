from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") > "2025-06-01")
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("avg_quantity") > 54)
)
