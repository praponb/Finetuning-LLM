from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 7, "LARGE").otherwise("SMALL"))
    .cube("order_id", "quantity_tier", "product_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.stddev_samp("unit_price").alias("unit_price_stddev"),
        F.percentile_approx("unit_price", 0.9).alias("unit_price_p90"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.stddev_samp("unit_price").alias("unit_price_stddev_2"),
        F.sum("unit_price").alias("total_unit_price"),
        F.grouping("order_id").alias("grouping_level"),
    )
    .filter(F.col("unit_price_p90") >= 0)
    .orderBy("total_unit_price")
)
