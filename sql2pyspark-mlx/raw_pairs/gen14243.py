from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .groupBy(
        F.when(F.col("unit_price") >= 320, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
    .filter(F.col("quantity_count") < 4)
    .select("unit_price_bucket", "quantity_count")
    .orderBy("unit_price_bucket", "quantity_count")
    .limit(5)
)
