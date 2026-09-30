from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").between(99, 165))
    .groupBy(
        F.when(F.col("quantity") >= 206, "high").when(F.col("quantity") >= 108, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .filter(F.col("min_quantity") < 140)
)
