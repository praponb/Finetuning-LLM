from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(~(F.col("store_id").isin([1, 5]) & F.col("product_id").isin([4, 7, 9, 10]) & F.col("product_id").isin([3, 6, 9, 10])))
    .groupBy("store_id")
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
