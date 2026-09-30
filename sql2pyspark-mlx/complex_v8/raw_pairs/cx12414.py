from pyspark.sql import functions as F

filtered_readings = df_meter_readings.filter(F.col("quality") == "OK")

enriched_site = (
    filtered_readings.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z2"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
    )
)

exploded_detail = (
    enriched_site.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.site_tariff",
        "s.site_zone",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

line_level_readings = (
    exploded_detail.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.site_tariff",
        "s.site_zone",
        "s.reading_id",
        "s.reading_quality",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    line_level_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "site_tariff",
        "site_zone",
        "reading_id",
        "reading_quality",
        "reading_id_2",
        "reading_read_at_2",
        "reading_kwh",
        "reading_quality_2",
        F.date_format(F.col("reading_read_at_2"), "yyyy-MM-dd HH:00").alias("reading_read_at_2_label"),
        F.lower(F.regexp_replace(F.trim(F.col("reading_quality")), " ", "_")).alias("reading_quality_slug"),
    )
    .orderBy(
        F.col("reading_kwh").desc(),
        "id",
        "reading_id",
        "reading_id_2",
    )
    .limit(10)
)
