from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.avg("quantity").alias("avg_quantity"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy("unit_price_bucket")
)
