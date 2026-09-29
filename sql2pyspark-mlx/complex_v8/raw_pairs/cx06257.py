from pyspark.sql import functions as F

valid = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.date_trunc("day", F.col("read_at")).alias("read_at_day"),
    )
)

site_id_stats = (
    valid
    .groupBy("site_id")
    .agg(F.percentile_approx("kwh", 0.5).alias("median_kwh"))
)

outliers = (
    valid.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.kwh") >= F.col("g.median_kwh"))
    .select(
        "s.*",
        "g.median_kwh",
        F.round(F.col("s.kwh") - F.col("g.median_kwh"), 2).alias("kwh_minus_median"),
    )
)

with_parent = (
    outliers.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.installed_kw").alias("site_installed_kw"))
)

joined = (
    with_parent.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") <= 373.3), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.read_at_day",
        "s.kwh_minus_median",
        "s.site_installed_kw",
        "si.site_name",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

derived = (
    joined
    .filter(F.col("quality").isin("OK", "EST"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "read_at_day",
        "kwh_minus_median",
        "site_installed_kw",
        "site_name",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.sum("voltage").alias("total_reading_voltage"),
        F.count("*").alias("reading_count"),
    )
)

with_stats = (
    derived.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.total_reading_voltage",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "read_at_day",
        "kwh_minus_median",
        "site_name",
        "quality_or_unknown",
        "total_reading_voltage",
        "reading_count",
        (F.col("reading_count") + F.col("site_installed_kw")).alias("reading_count_plus_site_installed_kw"),
        F.abs(F.col("reading_count") - 2).alias("reading_count_distance"),
    )
    .orderBy(F.col("read_at_day").desc_nulls_first(), "kwh", "id")
    .limit(5)
)
