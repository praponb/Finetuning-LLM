from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("order_id").isin([5, 12, 15]) & (F.col("quantity").between(3, 4) | F.col("unit_price").between(120, 220)))
)

order_items_stats = (
    base
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.stddev("quantity"), 2).alias("stddev_quantity"))
)

result = (
    order_items_stats
    .filter(F.col("stddev_quantity") < 23)
    .orderBy("unit_price_bucket", "stddev_quantity")
)
