from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id", "product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.sum("unit_price").alias("total_unit_price"),
    )
)
