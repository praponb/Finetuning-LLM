from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 170, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("quantity").alias("unique_orders"))
    .filter(F.col("unique_orders") > 9)
)
