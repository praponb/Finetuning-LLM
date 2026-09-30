from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 9, "high").when(F.col("quantity") >= 2, "mid").otherwise("low").alias("quantity_bucket"),
        F.when(F.col("unit_price") >= 430, "high").when(F.col("unit_price") >= 70, "mid").otherwise("low").alias("unit_price_bucket"),
        "order_id",
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
)
