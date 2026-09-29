from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull() & F.col("order_id").isin([15, 32, 33, 37]))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
)
