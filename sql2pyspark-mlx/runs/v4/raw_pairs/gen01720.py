from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.sum("quantity").alias("total_quantity"),
        F.count("*").alias("total_count"),
    )
)
