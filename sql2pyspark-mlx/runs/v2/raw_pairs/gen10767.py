from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("order_id") == 36) | F.col("product_id").isin([8, 16, 26, 31])))
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.round(F.avg("unit_price"), 2).alias("avg_unit_price"))
    .filter(F.col("avg_unit_price") > 39)
    .orderBy(F.col("order_id").desc(), "unit_price_bucket")
)
