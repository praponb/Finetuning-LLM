from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").between(230, 360) & (F.col("unit_price") < 130) & (F.col("unit_price") <= 270))
    .groupBy(
        F.when(F.col("unit_price") >= 410, "high").when(F.col("unit_price") >= 50, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
    )
    .orderBy(F.col("stddev_unit_price").desc())
)
