from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 8)
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)
