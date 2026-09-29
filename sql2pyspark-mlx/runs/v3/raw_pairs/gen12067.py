from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") > 260) & (F.col("product_id") == 27))
    .groupBy("product_id")
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("total_count"),
    )
    .orderBy("total_count")
)
