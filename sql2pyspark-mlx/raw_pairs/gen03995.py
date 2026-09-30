from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter(F.col("order_id").isin([12, 35, 38]) & (F.col("quantity") <= 5))
)

summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 130, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
)

result = summary
