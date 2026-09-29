from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("quantity").isNotNull() & (F.col("unit_price") >= 220))
    .groupBy(
        F.when(F.col("unit_price") >= 360, "high").when(F.col("unit_price") >= 180, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
    )
    .filter(F.col("max_unit_price") > 310)
    .orderBy(F.col("unit_price_bucket").desc())
)
