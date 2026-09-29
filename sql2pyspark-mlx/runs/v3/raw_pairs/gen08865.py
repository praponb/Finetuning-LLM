from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(120, 150) | (F.col("unit_price") <= 240))
    .groupBy("product_id")
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
)
