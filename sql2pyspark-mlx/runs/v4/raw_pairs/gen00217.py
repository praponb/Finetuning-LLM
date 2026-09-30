from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 37) | F.col("product_id").isNotNull())
    .groupBy("product_id")
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") > 3)
    .drop("unique_orders")
    .orderBy("stddev_unit_price")
)
