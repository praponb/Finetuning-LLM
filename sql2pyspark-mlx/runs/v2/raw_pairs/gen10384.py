from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("unit_price") < 310) & (F.col("unit_price") >= 230) & (F.col("order_id") == 24)))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.min("quantity").alias("min_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)
