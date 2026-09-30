from pyspark.sql import functions as F

recent_order_items = df_order_items.filter(F.col("quantity") >= 3)

summary = (
    recent_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("*").alias("num_order_items"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)

result = summary.orderBy("unique_products", "num_order_items")
