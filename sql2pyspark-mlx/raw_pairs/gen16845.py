from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 200, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_orders"))
    .filter(F.col("unique_orders") > 8)
    .select("unit_price_bucket", "unique_orders")
    .orderBy(F.col("unit_price_bucket").desc())
)
