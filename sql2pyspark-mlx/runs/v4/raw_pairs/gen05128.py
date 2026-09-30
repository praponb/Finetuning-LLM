from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 310, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("*").alias("order_item_count"))
    .filter(F.col("order_item_count") < 5)
    .orderBy("order_item_count", "unit_price_bucket", "quantity_bucket")
    .limit(10)
)
