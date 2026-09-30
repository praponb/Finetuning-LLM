from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 370, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.avg("unit_price").alias("avg_unit_price"))
    .orderBy(F.col("avg_unit_price").desc(), "unit_price_bucket")
)
