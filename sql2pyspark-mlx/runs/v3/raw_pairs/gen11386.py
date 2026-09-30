from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 18, 20]))
    .groupBy("product_id", "order_id")
    .agg(
        F.count("unit_price").alias("unit_price_count"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy("unit_price_count")
)
