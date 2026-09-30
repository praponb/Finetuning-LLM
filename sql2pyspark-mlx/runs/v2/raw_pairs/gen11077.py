from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("order_id") == 28) & F.col("product_id").isNull())
    .agg(
        F.count("*").alias("total_count"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("unit_price").alias("unit_price_count"),
    )
)
