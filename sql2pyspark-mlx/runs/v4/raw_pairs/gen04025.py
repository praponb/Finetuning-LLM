from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("order_id") == 2)
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

order_items_stats = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price_int") >= 87, "high").when(F.col("unit_price_int") >= 21, "mid").otherwise("low").alias("unit_price_int_bucket"),
        "product_id",
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.count("*").alias("row_count"),
    )
)

result = order_items_stats
