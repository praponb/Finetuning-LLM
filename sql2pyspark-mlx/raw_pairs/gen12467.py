from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") > 320)
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("*").alias("order_item_count"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
