from pyspark.sql import functions as F

result = (
    df_order_items
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), F.col("avg_quantity").desc())
)
