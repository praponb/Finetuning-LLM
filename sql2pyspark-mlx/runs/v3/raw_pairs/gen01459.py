from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([3, 31]))
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 100, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.countDistinct("quantity").alias("unique_orders"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("unique_orders") < 7)
    .select("unit_price_bucket", "unique_orders", "cnt")
)
