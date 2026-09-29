from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.min("unit_price").alias("min_unit_price"),
    )
    .filter(F.col("min_unit_price") > 180)
    .drop("min_unit_price")
    .orderBy("stddev_unit_price", "quantity_bucket")
    .limit(3)
)
