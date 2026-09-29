from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 202, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("event_count"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
    )
    .orderBy(F.col("quantity_p90").asc_nulls_first())
)
