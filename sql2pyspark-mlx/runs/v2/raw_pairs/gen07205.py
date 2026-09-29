from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(110, 250) & (F.col("quantity") == 8))
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)
