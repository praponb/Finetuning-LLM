from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")) & (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

with_counts = (
    base.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

totals = (
    with_counts
    .filter(F.col("voltage") <= 240.1)
    .groupBy("reading_count", "site_id")
    .agg(
        F.round(F.avg("reading_count"), 2).alias("avg_reading_count"),
        F.sum("kwh").alias("total_kwh"),
        F.sum("reading_count").alias("total_reading_count"),
        F.count(F.when(F.col("quality") != "EST", 1)).alias("n_quality_est"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("EST", "OK"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_est_ok"),
        F.max("read_at").alias("last_read_at"),
        F.sum("voltage").alias("sum_voltage_check"),
    )
    .filter(F.col("sum_voltage_check") > 218.2)
    .drop("sum_voltage_check")
)

annotated = (
    totals
    .filter(F.col("site_id").isNotNull())
    .select(
        "reading_count",
        "site_id",
        "avg_reading_count",
        "total_kwh",
        "n_quality_est",
        "pct_quality_not_in_est_ok",
        F.date_trunc("day", F.col("last_read_at")).alias("last_read_at_day"),
        F.when(F.col("n_quality_est") >= 6, "HIGH").when(F.col("n_quality_est") >= 2, "MEDIUM").otherwise("LOW").alias("n_quality_est_tier"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") > 26.9), "left_semi")
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count("*").alias("reading_count_2"),
        F.min("read_at").alias("first_reading_read_at_2"),
    )
)

profiled = (
    screened.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
        "m.first_reading_read_at_2",
    )
)

line_level_readings = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.reading_count",
        "s.site_id",
        "s.avg_reading_count",
        "s.total_kwh",
        "s.n_quality_est",
        "s.pct_quality_not_in_est_ok",
        "s.n_quality_est_tier",
        "s.first_reading_read_at",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

result = (
    line_level_readings
    .filter(F.col("pct_quality_not_in_est_ok") <= 60.0)
    .select(
        "reading_count",
        "site_id",
        "avg_reading_count",
        "total_kwh",
        "n_quality_est",
        "pct_quality_not_in_est_ok",
        "n_quality_est_tier",
        "first_reading_read_at",
        "reading_id",
        "reading_kwh",
        "reading_quality",
        "reading_read_at",
        F.lower(F.regexp_replace(F.trim(F.col("n_quality_est_tier")), " ", "_")).alias("n_quality_est_tier_slug"),
        F.round(F.col("avg_reading_count") * 0.25, 2).alias("avg_reading_count_adj"),
    )
    .orderBy(
        F.col("n_quality_est").desc_nulls_first(),
        F.col("reading_read_at").desc(),
    )
)
