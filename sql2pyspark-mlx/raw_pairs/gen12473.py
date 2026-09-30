from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([7, 8, 26, 38]))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.countDistinct("unit_price").alias("unique_orders"),
        F.countDistinct("unit_price").alias("unique_products"),
    )
)
