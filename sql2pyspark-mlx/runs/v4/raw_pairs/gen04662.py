from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2025-03-01")
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy("store_id")
)
