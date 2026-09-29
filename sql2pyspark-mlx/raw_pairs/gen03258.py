from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 5]))
    .rollup("product_id", F.year("last_restock").alias("last_restock_year"))
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc(), F.col("last_restock_year").desc())
)
