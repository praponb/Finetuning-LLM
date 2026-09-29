from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 420, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.count("quantity").alias("quantity_count"),
    )
    .filter(F.col("quantity_count") < 1)
    .orderBy("avg_unit_price", F.col("quantity_count").desc())
)
