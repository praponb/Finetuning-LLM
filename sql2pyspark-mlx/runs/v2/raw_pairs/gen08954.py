from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 350, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.avg("unit_price").alias("avg_unit_price"),
        F.countDistinct("quantity").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") < 9)
    .select("unit_price_bucket", "avg_unit_price", "unique_orders")
)
