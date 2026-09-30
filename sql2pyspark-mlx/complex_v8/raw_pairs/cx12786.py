from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 6) | (~F.col("quality").isin("BAD", "EST")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 236.8, "A").when(F.col("voltage") >= 222.1, "B").otherwise("C").alias("voltage_tier"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

line_level = (
    filtered_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 236.8), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.read_at_hour",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality"),
    )
)

site_id_stats = (
    line_level
    .groupBy("site_id")
    .agg(F.percentile_approx("read_at_hour", 0.5).alias("median_read_at_hour"))
)

vs_group = (
    line_level.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.read_at_hour") >= F.col("g.median_read_at_hour"))
    .select(
        "s.*",
        "g.median_read_at_hour",
        F.round(F.col("s.read_at_hour") - F.col("g.median_read_at_hour"), 2).alias("read_at_hour_minus_median"),
    )
)

with_details = (
    vs_group.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.installed_kw") >= 96.9)
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.read_at_hour",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_voltage",
        "s.reading_kwh",
        "s.reading_quality",
        "s.median_read_at_hour",
        "s.read_at_hour_minus_median",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    with_details
    .groupBy("read_at_hour")
    .pivot("reading_quality", ["EST", "OK", "BAD"])
    .agg(F.sum("read_at_hour_minus_median").alias("total"), F.max("read_at_hour_minus_median").alias("max_v"))
    .orderBy("read_at_hour")
)
