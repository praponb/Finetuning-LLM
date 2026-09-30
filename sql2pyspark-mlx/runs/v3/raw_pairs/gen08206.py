from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
        "product_id",
    )
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 2)
    .orderBy("stddev_quantity", "unit_price_bucket", "product_id")
    .limit(3)
)
