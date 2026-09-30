from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 3, 4, 9]))
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("last_restock").alias("min_last_restock"),
    )
    .filter(F.col("stddev_quantity") > 31)
    .orderBy(F.col("stddev_quantity").desc())
)
