from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4, 6]) & F.col("product_id").isin([3, 8]))
)

summary = (
    base
    .groupBy("store_id", "product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)

result = (
    summary
    .filter(F.col("max_quantity") < 60)
    .orderBy("store_id")
)
