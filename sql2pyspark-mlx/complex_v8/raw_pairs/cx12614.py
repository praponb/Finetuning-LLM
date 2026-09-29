from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("voltage") >= 224.8) & (F.col("quality") != "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

enriched = (
    recent
    .select(
        "*",
        F.when(F.col("voltage") >= 232.0, "A").when(F.col("voltage") >= 224.9, "B").otherwise("C").alias("voltage_tier"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

exploded_detail = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_week",
        "s.quality_or_unknown",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

qualified = (
    exploded_detail.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 232.7) & (F.col("m.read_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")), "left_anti")
)

scored = (
    qualified
    .filter(F.col("reading_voltage") < 236.8)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "read_at_week",
        "quality_or_unknown",
        "reading_id",
        F.substring(F.trim(F.col("quality_or_unknown")), 1, 2).alias("quality_or_unknown_prefix"),
        F.when(F.col("voltage") >= 233.8, "A").when(F.col("voltage") >= 218.4, "B").otherwise("C").alias("voltage_tier"),
    )
)

with_children_readings = (
    scored.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 218.2), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality_or_unknown",
        "s.reading_id",
        "s.voltage_tier",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    with_children_readings
    .groupingSets(
        [["quality_or_unknown", "site_id"], ["quality_or_unknown"], ["site_id"]],
        "quality_or_unknown", "site_id",
    )
    .agg(F.countDistinct("reading_id").alias("distinct_reading_ids"))
    .orderBy(
        F.col("quality_or_unknown").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
