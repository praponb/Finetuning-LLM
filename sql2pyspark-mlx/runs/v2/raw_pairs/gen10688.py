from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
    .filter(F.col("stddev_quantity") < 45)
    .select("product_id", "unit_price_bucket", "stddev_quantity")
    .orderBy(F.col("stddev_quantity").desc())
)
