from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)
