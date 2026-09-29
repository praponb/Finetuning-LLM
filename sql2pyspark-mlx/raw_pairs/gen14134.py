from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 110, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.countDistinct("unit_price").alias("unique_products"))
    .filter(F.col("unique_products") > 10)
    .select("order_id", "unit_price_bucket", "unique_products")
    .orderBy("unit_price_bucket", "order_id")
    .limit(1)
)
