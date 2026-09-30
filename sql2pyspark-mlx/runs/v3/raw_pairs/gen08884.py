from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") <= 6) & F.col("order_id").isin([4, 10, 17, 26]))
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)
