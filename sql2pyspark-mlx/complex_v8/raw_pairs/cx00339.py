from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter((F.col("voltage") >= 219.6) & (F.col("read_at") < F.lit("2025-02-24 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.month(F.col("read_at")).alias("read_at_month"),
    )
)

quality_or_unknown_stats = (
    valid
    .groupBy("quality_or_unknown")
    .agg(F.avg("read_at_month").alias("avg_read_at_month"))
)

outliers = (
    valid.alias("s")
    .join(quality_or_unknown_stats.alias("g"), F.col("g.quality_or_unknown") == F.col("s.quality_or_unknown"), "inner")
    .filter(F.col("s.read_at_month") > F.col("g.avg_read_at_month"))
    .select(
        "s.*",
        "g.avg_read_at_month",
        F.round(F.col("s.read_at_month") - F.col("g.avg_read_at_month"), 2).alias("read_at_month_minus_avg"),
    )
)

scored = (
    outliers
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown_2"),
        (F.floor(F.col("avg_read_at_month") / 2) * 2).alias("avg_read_at_month_bucket"),
    )
)

line_level_readings = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.to_date(F.col("m.read_at")) < F.lit("2025-03-08").cast("date")), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality",
        "s.quality_or_unknown",
        "s.read_at_month",
        "s.avg_read_at_month",
        "s.read_at_month_minus_avg",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

exploded_detail_readings = (
    line_level_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.month(F.col("m.read_at")).isin(1, 3)), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.quality_or_unknown",
        "s.read_at_month_minus_avg",
        "s.reading_id",
        "s.reading_read_at",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.kwh").alias("reading_kwh_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

grouped = (
    exploded_detail_readings
    .groupBy("site_id", "quality_or_unknown")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(100 * F.count(F.when(F.col("quality") == "BAD", 1)) / F.count("*"), 1).alias("pct_quality_bad"),
    )
)

annotated = (
    grouped
    .select(
        "site_id",
        "quality_or_unknown",
        F.when(F.col("quality_or_unknown").isin("UNKNOWN", "EST"), "group_a").when(F.col("quality_or_unknown") == "OK", "group_b").otherwise("other").alias("quality_or_unknown_group"),
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
)

result = (
    annotated
    .groupingSets(
        [["quality_or_unknown", "site_id"], ["quality_or_unknown"]],
        "quality_or_unknown", "site_id",
    )
    .agg(
        F.grouping("quality_or_unknown").alias("is_total_quality_or_unknown"),
        F.count("*").alias("n_rows"),
    )
)
