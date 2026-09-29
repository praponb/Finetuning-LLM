from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("quantity") != 4) & (F.col("unit_price") <= 240) & (F.col("order_id") == 34))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 160, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)
