from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("unit_price") >= 330, "high").when(F.col("unit_price") >= 80, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_products"))
    .filter(F.col("unique_products") < 6)
    .orderBy("unit_price_bucket")
)
