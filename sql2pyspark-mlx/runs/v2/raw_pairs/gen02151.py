from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 7]))
    .groupBy("store_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("num_inventory"),
    )
    .filter(F.col("stddev_quantity") >= 30)
    .select("store_id", "stddev_quantity", "num_inventory")
)
