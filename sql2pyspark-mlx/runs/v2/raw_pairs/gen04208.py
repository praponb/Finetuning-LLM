from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
        "product_id",
    )
    .agg(
        F.count("*").alias("order_item_count"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .orderBy("max_unit_price", "quantity_bucket", "product_id")
    .limit(10)
)
