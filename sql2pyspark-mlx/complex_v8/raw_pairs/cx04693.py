from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 9) | (F.col("kwh") < 106.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.col("read_at") + F.expr("INTERVAL 2 HOURS")).alias("read_at_plus_2h"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group"),
    )
)

detail_rows = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

enriched = (
    detail_rows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.quality_len",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality",
        "s.reading_voltage",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

joined_reading = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "inner")
    .filter(F.col("m.quality").isNotNull())
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality_len",
        "s.reading_id",
        "s.reading_read_at",
        "s.reading_quality",
        "s.reading_voltage",
        "s.site_tariff",
        "s.site_commissioned_on",
        "s.reading_read_at_2",
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
        F.col("m.read_at").alias("reading_read_at_3"),
    )
)

per_group = (
    joined_reading
    .groupBy(
        "quality_len",
        F.date_trunc("month", "site_commissioned_on").alias("site_commissioned_on_month"),
        "site_id",
    )
    .agg(
        F.max("reading_read_at").alias("last_reading_read_at"),
        F.percentile_approx("voltage", 0.5).alias("median_voltage"),
        F.avg("reading_voltage").alias("avg_reading_voltage_check"),
    )
    .filter(F.col("avg_reading_voltage_check") > 218.2)
    .drop("avg_reading_voltage_check")
)

result = (
    per_group
    .select(
        "quality_len",
        "site_commissioned_on_month",
        "site_id",
        "last_reading_read_at",
        "median_voltage",
    )
    .orderBy(F.col("quality_len").desc(), "site_commissioned_on_month", "site_id")
    .limit(10)
)
