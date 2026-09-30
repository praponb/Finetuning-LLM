from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") >= 320)
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 150, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.count("quantity").alias("quantity_count"),
        F.max("unit_price").alias("max_unit_price"),
    )
    .filter(F.col("quantity_count") < 6)
    .select("unit_price_bucket", "quantity_count", "max_unit_price")
    .orderBy("unit_price_bucket", F.col("max_unit_price").desc())
    .limit(1)
)
