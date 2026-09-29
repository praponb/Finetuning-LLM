from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("*").alias("num_order_items"),
        F.count("*").alias("total_count"),
    )
    .orderBy("product_id", F.col("total_count").desc())
)
