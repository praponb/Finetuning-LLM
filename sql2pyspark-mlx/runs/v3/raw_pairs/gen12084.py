from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 8, 27]) & F.col("product_id").isin([16, 28, 37]))
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("num_order_items") < 1)
    .drop("num_order_items")
)
