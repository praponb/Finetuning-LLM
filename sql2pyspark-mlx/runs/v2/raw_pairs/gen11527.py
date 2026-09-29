from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.countDistinct("unit_price").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") < 4)
    .orderBy(F.col("avg_unit_price").desc())
)
