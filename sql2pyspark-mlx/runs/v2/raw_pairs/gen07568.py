from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .orderBy(F.col("min_launch_date").desc(), F.col("stddev_price").desc())
)
