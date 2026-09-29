from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.col("shipped_date").between("2024-12-05", "2025-01-25"))
    .withColumn("cost_scaled", F.round((F.col("cost") * 0.85), 2))
    .withColumn("status_length", F.length("status"))
)

result = (
    cleaned
    .groupBy(
        F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"),
        F.date_trunc("WEEK", "shipped_date").alias("week_start"),
        F.when(F.col("cost") >= 98, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("status_length"), 2).alias("avg_status_length"),
    )
    .filter(F.col("distinct_order_ids") > 1)
    .orderBy(F.col("avg_status_length").desc_nulls_last())
)
