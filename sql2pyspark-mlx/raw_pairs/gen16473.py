from pyspark.sql import functions as F

filtered_order_items = (
    df_order_items
    .filter((F.col("unit_price") >= 350) & F.col("order_id").isin([5, 18, 25]))
)

summary = (
    filtered_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 190, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("quantity").alias("avg_quantity"))
)

result = summary.filter(F.col("avg_quantity") >= 6.1)
