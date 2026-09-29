from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("a")
    .join(
        df_meter_readings.alias("b"),
        (F.col("b.site_id") == F.col("a.site_id"))
        & (F.col("b.read_at") < F.col("a.read_at"))
        & (F.col("b.read_at") >= F.col("a.read_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.site_id", "a.read_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.voltage").alias("prior_voltage_1d"),
    )
)

labeled = (
    matched
    .withColumn("prior_rows_1d_tier", F.when(F.col("prior_rows_1d") >= 5, "HIGH").when(F.col("prior_rows_1d") >= 2, "MEDIUM").otherwise("LOW"))
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.max("prior_rows_1d").alias("overall_max_prior_rows_1d")))
    .filter(F.col("prior_rows_1d") < F.col("overall_max_prior_rows_1d"))
    .drop("overall_max_prior_rows_1d")
)

enriched = (
    filtered
    .filter(~F.col("prior_rows_1d_tier").isin("LOW", "HIGH"))
    .select(
        "*",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.coalesce(F.col("prior_voltage_1d"), F.lit(0.0)).alias("prior_voltage_1d_filled"),
    )
)

result = (
    enriched
    .filter(F.col("prior_rows_1d") <= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "prior_voltage_1d",
        "prior_rows_1d_tier",
        "days_since_read_at",
        "prior_voltage_1d_filled",
    )
    .orderBy(F.col("read_at").desc_nulls_first(), "id")
    .limit(20)
)
