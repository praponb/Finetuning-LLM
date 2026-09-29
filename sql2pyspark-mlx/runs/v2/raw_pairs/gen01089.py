from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2024-01-01")
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("product_id").desc(), F.col("avg_quantity").desc())
)
