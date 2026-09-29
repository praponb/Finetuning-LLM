from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id", "order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.count("quantity").alias("quantity_count"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") > 39)
    .orderBy("product_id")
)
