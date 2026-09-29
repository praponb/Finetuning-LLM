from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(F.count("*").alias("order_item_count"))
    .filter(F.col("order_item_count") >= 10)
    .select("quantity_bucket", "product_id", "order_item_count")
)
