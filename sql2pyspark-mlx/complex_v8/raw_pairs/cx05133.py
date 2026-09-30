from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

without_matches = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.kwh") < 109.0) & (F.col("m.quality") == "EST"), "left_anti")
)

joined_site = (
    without_matches.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

line_level = (
    joined_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality") == "EST"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.site_name",
        "s.site_commissioned_on",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

scored = (
    line_level
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "site_commissioned_on",
        "reading_id",
        "reading_quality",
        F.abs(F.col("kwh") - 79.7).alias("kwh_distance"),
        (F.col("reading_read_at") + F.expr("INTERVAL 2 HOURS")).alias("reading_read_at_plus_2h"),
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("site_id").cast("string")).alias("reading_quality_ref"),
    )
)

quality_stats = (
    scored
    .groupBy("quality")
    .agg(F.avg("voltage").alias("avg_voltage"))
)

above_average = (
    scored.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.voltage") > F.col("g.avg_voltage"))
    .select(
        "s.*",
        "g.avg_voltage",
        F.round(F.col("s.voltage") - F.col("g.avg_voltage"), 2).alias("voltage_minus_avg"),
    )
)

enriched = (
    above_average.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("m.site_id"), "left")
    .select(
        "s.id",
        "s.voltage",
        "s.site_commissioned_on",
        "s.reading_id",
        "s.reading_quality",
        "s.kwh_distance",
        "s.reading_read_at_plus_2h",
        "s.voltage_minus_avg",
        F.col("m.site_id").alias("reading_site_id"),
        "si.site_name",
    )
)

result = (
    enriched
    .select(
        "id",
        "voltage",
        "site_commissioned_on",
        "reading_id",
        "reading_quality",
        "voltage_minus_avg",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
    )
    .orderBy("voltage", "id", "reading_id")
    .limit(10)
)
