from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 232, "high").when(F.col("quantity") >= 113, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 55)
)
