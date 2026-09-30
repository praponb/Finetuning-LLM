from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("num_inventory") > 9)
    .select("store_id", "stddev_quantity", "num_inventory")
)
