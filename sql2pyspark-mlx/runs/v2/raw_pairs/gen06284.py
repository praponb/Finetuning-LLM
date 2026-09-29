from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([15, 35]))
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 60, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") < 120)
    .orderBy("unit_price_bucket")
)
