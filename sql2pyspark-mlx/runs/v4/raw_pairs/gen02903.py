from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-08-12").cast("date"))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

result = (
    valid
    .groupBy(
        F.when(F.col("quantity") >= 210, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_filled", 0.5).alias("quantity_filled_p50"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
    )
    .filter(F.col("quantity_filled_p50") > 0)
    .orderBy(F.col("distinct_product_ids").desc())
)
