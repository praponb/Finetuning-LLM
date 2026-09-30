from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 9))
    .withColumn("least_quantity_unit_price", F.least("quantity", "unit_price"))
    .withColumn("line_total", F.round((F.col("quantity") / F.col("unit_price")), 2))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("unit_price") >= 340, "HIGH").otherwise("LOW").alias("unit_price_tier"),
    )
    .agg(
        F.percentile_approx("unit_price", 0.5).alias("unit_price_p50"),
        F.percentile_approx("least_quantity_unit_price", 0.95).alias("least_quantity_unit_price_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("cnt"),
        F.percentile_approx("least_quantity_unit_price", 0.9).alias("least_quantity_unit_price_p90"),
    )
    .orderBy(F.col("unit_price_p50").desc())
)
