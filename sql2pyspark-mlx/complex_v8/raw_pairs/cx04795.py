from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

qualified = (
    recent
    .crossJoin(recent.agg(F.max("voltage").alias("overall_max_voltage")))
    .filter(F.col("voltage") < F.col("overall_max_voltage"))
    .drop("overall_max_voltage")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(~F.col("quality").isin("EST", "OK"), 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    qualified.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

stats = (
    with_stats
    .groupBy(
        "site_id",
        "quality",
        F.date_trunc("month", "first_reading_read_at").alias("first_reading_read_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.sum("voltage") / F.nullif(F.sum("reading_quality_count"), F.lit(0)), 3).alias("voltage_to_reading_quality_count_ratio"),
        F.round(F.stddev("reading_quality_count"), 2).alias("stddev_reading_quality_count"),
    )
)

enriched = (
    stats
    .select(
        "*",
        F.array_join(F.array_sort(F.col("quality_list")), ",").alias("quality_list_text"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.year(F.col("last_read_at")).alias("last_read_at_year"),
    )
)

result = (
    enriched
    .filter(F.col("first_reading_read_at_month") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
    .select(
        "site_id",
        "quality",
        "first_reading_read_at_month",
        "voltage_to_reading_quality_count_ratio",
        "quality_list_text",
        "quality_or_unknown",
        "last_read_at_year",
        F.coalesce(F.col("quality_or_unknown"), F.lit("UNKNOWN")).alias("quality_or_unknown_or_unknown"),
    )
    .orderBy("quality_list_text")
)
