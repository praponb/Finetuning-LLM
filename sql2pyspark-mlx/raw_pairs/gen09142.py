from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([7, 18, 39]) & (F.col("quantity") != 8))
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.avg("unit_price").alias("avg_unit_price"),
    )
)
