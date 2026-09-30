from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 22) | F.col("order_id").isin([6, 7, 23, 28]))
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
        (F.max("unit_price") - F.min("unit_price")).alias("unit_price_range"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 4)
    .drop("unique_orders")
    .orderBy(F.col("unit_price_range").desc(), "product_id")
    .limit(20)
)
