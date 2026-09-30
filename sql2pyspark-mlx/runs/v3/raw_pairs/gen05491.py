from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity") > 5)
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)
