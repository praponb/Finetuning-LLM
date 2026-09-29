from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 21, 30]))
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("stddev_unit_price") < 58)
)
