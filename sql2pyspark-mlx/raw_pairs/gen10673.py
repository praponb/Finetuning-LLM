from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum("quantity").alias("total_quantity"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("total_quantity") < 28)
    .orderBy("total_quantity", "order_id", "unit_price_bucket")
    .limit(50)
)
