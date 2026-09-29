from pyspark.sql import functions as F

result = (
    df_order_items
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 350, "HIGH").otherwise("LOW"))
    .cube("unit_price_tier", "product_id")
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price_2"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.count("*").alias("cnt"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("quantity_bit_and") >= 1)
)
