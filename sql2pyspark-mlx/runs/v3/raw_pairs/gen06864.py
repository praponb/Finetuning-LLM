from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 33) & (F.col("order_id") == 15))
    .groupBy("order_id")
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("revenue") >= 51)
    .orderBy(F.col("revenue").desc())
)
