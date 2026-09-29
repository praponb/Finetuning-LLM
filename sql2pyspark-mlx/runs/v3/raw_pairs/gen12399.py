from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") < 170)
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 140, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.countDistinct("unit_price").alias("unique_products"),
        F.count("*").alias("num_order_items"),
    )
)
