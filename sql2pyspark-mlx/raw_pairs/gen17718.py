from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 5]))
    .groupBy(
        F.when(F.col("quantity") >= 217, "high").when(F.col("quantity") >= 84, "mid").otherwise("low").alias("quantity_bucket"),
        "store_id",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("avg_quantity").desc())
)
