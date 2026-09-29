from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.min("last_restock").alias("min_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("stddev_quantity").desc())
)
