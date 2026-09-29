from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([8, 25, 35]) & F.col("order_id").isin([2, 6, 12, 18]))
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_orders"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)
