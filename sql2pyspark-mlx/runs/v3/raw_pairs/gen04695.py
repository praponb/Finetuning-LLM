from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(
        F.countDistinct("quantity").alias("unique_products"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .orderBy(F.col("unique_products").desc())
)
