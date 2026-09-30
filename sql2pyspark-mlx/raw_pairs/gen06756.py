from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.max("last_restock").alias("last_last_restock"),
        F.max("last_restock").alias("latest_last_restock"),
    )
    .orderBy(F.col("stddev_quantity").desc(), "store_id")
    .limit(5)
)
