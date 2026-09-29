from pyspark.sql import functions as F

filtered = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.add_months(F.col("commissioned_on"), 3).alias("commissioned_on_plus_3m"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("voltage").alias("max_reading_voltage"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.max_reading_voltage")
)

zone_or_unknown_stats = (
    summarized_rows
    .groupBy("zone_or_unknown")
    .agg(F.avg("max_reading_voltage").alias("avg_max_reading_voltage"))
)

outliers = (
    summarized_rows.alias("s")
    .join(zone_or_unknown_stats.alias("g"), F.col("g.zone_or_unknown") == F.col("s.zone_or_unknown"), "inner")
    .filter(F.col("s.max_reading_voltage") > F.col("g.avg_max_reading_voltage"))
    .select(
        "s.*",
        "g.avg_max_reading_voltage",
        F.round(F.col("s.max_reading_voltage") - F.col("g.avg_max_reading_voltage"), 2).alias("max_reading_voltage_minus_avg"),
    )
)

annotated = (
    outliers
    .select(
        "*",
        (F.col("installed_kw") - F.col("max_reading_voltage")).alias("installed_kw_minus_max_reading_voltage"),
        F.greatest(F.col("avg_max_reading_voltage"), F.col("max_reading_voltage")).alias("greatest_avg_max_reading_voltage_max_reading_vol"),
        F.lower(F.regexp_replace(F.trim(F.col("zone_or_unknown")), " ", "_")).alias("zone_or_unknown_slug"),
    )
)

scored = (
    annotated
    .select(
        "*",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
        F.coalesce(F.col("avg_max_reading_voltage"), F.lit(0.0)).alias("avg_max_reading_voltage_filled"),
    )
)

reading_stats_2 = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count(F.when(F.col("quality") == "BAD", 1)).alias("reading_quality_count"),
        F.min("read_at").alias("first_reading_read_at_2"),
        F.round(F.avg("voltage"), 2).alias("avg_reading_voltage"),
    )
)

with_stats = (
    scored.alias("s")
    .join(reading_stats_2.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
        "m.first_reading_read_at_2",
        "m.avg_reading_voltage",
    )
)

labeled = (
    with_stats
    .select(
        "*",
        F.concat_ws("|", F.col("zone"), F.col("commissioned_on_label")).alias("zone_commissioned_on_label_label"),
        F.initcap(F.trim(F.col("zone_or_unknown"))).alias("zone_or_unknown_clean"),
    )
)

result = (
    labeled
    .filter(F.col("avg_reading_voltage").between(220.5, 232.8))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        "commissioned_on_plus_3m",
        "zone_or_unknown",
        "max_reading_voltage",
        "max_reading_voltage_minus_avg",
        "installed_kw_minus_max_reading_voltage",
        "greatest_avg_max_reading_voltage_max_reading_vol",
        "zone_or_unknown_slug",
        "commissioned_on_label",
        "avg_max_reading_voltage_filled",
        "reading_quality_count",
        "first_reading_read_at_2",
        "avg_reading_voltage",
        "zone_commissioned_on_label_label",
        "zone_or_unknown_clean",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
        F.length(F.trim(F.col("zone_or_unknown"))).alias("zone_or_unknown_len"),
    )
)
