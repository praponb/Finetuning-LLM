from pyspark.sql import functions as F

result = (
    df_inventory
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
)
