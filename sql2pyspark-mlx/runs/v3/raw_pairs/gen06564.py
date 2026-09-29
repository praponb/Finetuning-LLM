from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("quantity").isNotNull())
    .groupBy("product_id")
    .agg(
        F.count("*").alias("num_inventory"),
        F.min("quantity").alias("min_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .orderBy(F.col("min_quantity").desc(), "num_inventory")
)
