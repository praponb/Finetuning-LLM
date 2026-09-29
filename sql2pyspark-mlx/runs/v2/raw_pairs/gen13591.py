from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([1, 2, 3, 6]) & F.col("store_id").isin([3, 6]))
    .groupBy("store_id", "product_id")
    .agg(
        F.max("last_restock").alias("latest_last_restock"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.countDistinct("quantity").alias("unique_products"),
    )
    .orderBy("latest_last_restock", F.col("product_id").desc(), "store_id")
    .limit(1)
)
