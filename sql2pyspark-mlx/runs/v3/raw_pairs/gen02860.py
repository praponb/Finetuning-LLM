from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"))
    .filter(F.col("stddev_unit_price") < 46)
    .select("quantity_bucket", "stddev_unit_price")
    .orderBy("quantity_bucket")
)
