from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
)
