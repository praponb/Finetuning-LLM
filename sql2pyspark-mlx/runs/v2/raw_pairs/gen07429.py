from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy(F.col("unique_products").desc(), "store_id", "product_id")
    .limit(1)
)
