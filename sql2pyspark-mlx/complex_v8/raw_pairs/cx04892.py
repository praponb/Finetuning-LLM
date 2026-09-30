from pyspark.sql import functions as F

scoped = (
    df_sites
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") >= 15.9) & (F.month(F.col("m.read_at")).isin(2, 3)), "left_semi")
    .filter(~F.col("tariff").isin("EV", "TOU"))
)

with_children_readings = (
    without_matches.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.tariff",
        "s.installed_kw",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

joined_reading = (
    with_children_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.tariff",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality",
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

enriched = (
    joined_reading
    .filter(F.col("reading_id").isNotNull())
    .select(
        "id",
        "tariff",
        "reading_id",
        "reading_read_at",
        "reading_quality",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
        F.substring(F.trim(F.col("reading_quality")), 1, 2).alias("reading_quality_prefix"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.concat_ws("|", F.col("tariff_or_unknown"), F.col("tariff")).alias("tariff_or_unknown_tariff_label"),
        F.quarter(F.col("reading_read_at")).alias("reading_read_at_quarter"),
        F.substring(F.trim(F.col("reading_quality")), 1, 3).alias("reading_quality_prefix_2"),
    )
)

enriched_2 = (
    labeled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.tariff",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality",
        "s.tariff_or_unknown",
        "s.reading_quality_prefix",
        "s.reading_read_at_quarter",
        "s.reading_quality_prefix_2",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "tariff",
        "reading_id",
        "reading_read_at",
        "reading_read_at_quarter",
        "reading_quality_prefix_2",
        "reading_site_id",
        "reading_kwh",
        "reading_quality_2",
        F.concat_ws(" / ", F.col("reading_quality_2"), F.col("reading_quality_prefix_2")).alias("reading_quality_2_reading_quality_prefix_2_label"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reading_read_at")).alias("days_since_reading_read_at"),
    )
)
