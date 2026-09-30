from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter((F.col("quantity") <= 3) | F.col("product_id").isin([13, 21, 30, 36]))
)

summary = (
    filtered_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 390, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)

result = (
    summary
    .filter(F.col("total_unit_price") > 1400)
    .orderBy("order_id", "unit_price_bucket")
    .limit(3)
)
