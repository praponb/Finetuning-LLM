from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
    .filter(F.col("max_quantity") < 90)
    .select("store_id", "max_quantity", "stddev_quantity")
    .orderBy("store_id", "stddev_quantity")
    .limit(1)
)
