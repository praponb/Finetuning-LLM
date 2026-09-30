from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 7) & (F.col("voltage") >= 219.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

detail_rows = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"),
        F.min("read_at").alias("first_reading_read_at"),
        F.count(F.when(F.col("quality") == "OK", 1)).alias("reading_quality_count"),
    )
)

with_stats = (
    detail_rows.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.avg_reading_kwh",
        "m.first_reading_read_at",
        F.coalesce(F.col("m.reading_quality_count"), F.lit(0)).alias("reading_quality_count"),
    )
)

derived = (
    with_stats
    .filter(F.col("reading_kwh") <= 109.0)
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "reading_id",
        "reading_read_at",
        "reading_voltage",
        "reading_quality",
        "avg_reading_kwh",
        "first_reading_read_at",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.concat(F.col("reading_quality"), F.lit("#"), F.col("id").cast("string")).alias("reading_quality_ref"),
        F.coalesce(F.col("reading_quality"), F.lit("UNKNOWN")).alias("reading_quality_or_unknown"),
    )
)

tagged = (
    derived
    .select(
        "*",
        (F.floor(F.col("avg_reading_kwh") / 25) * 25).alias("avg_reading_kwh_bucket"),
        F.initcap(F.trim(F.col("reading_quality_ref"))).alias("reading_quality_ref_clean"),
    )
)

enriched = (
    tagged.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.reading_id",
        "s.reading_voltage",
        "s.reading_quality",
        "s.avg_reading_kwh",
        "s.first_reading_read_at",
        "s.quality_slug",
        "s.avg_reading_kwh_bucket",
        "s.reading_quality_ref_clean",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

without_matches = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.id") == F.col("s.reading_id")) & (F.col("m.voltage").between(213.7, 241.9)), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "reading_id",
        "reading_voltage",
        "reading_quality",
        "avg_reading_kwh",
        "avg_reading_kwh_bucket",
        "reading_quality_ref_clean",
        "site_tariff",
        "site_commissioned_on",
        F.round(F.col("reading_voltage") * 1.1, 2).alias("reading_voltage_adj"),
    )
    .orderBy(F.col("site_tariff").desc(), F.col("reading_voltage").desc())
)
