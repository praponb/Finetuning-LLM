from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("num_order_items"),
    )
    .filter(F.col("revenue") > 77)
)
