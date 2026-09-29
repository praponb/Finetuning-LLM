from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("store_id").isin([2, 3, 4]))
    .groupBy("store_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("store_id").desc(), "min_quantity")
)
