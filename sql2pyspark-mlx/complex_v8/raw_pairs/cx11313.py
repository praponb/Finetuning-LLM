from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter(F.col("voltage") > 220.7)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.greatest(F.col("voltage"), F.col("kwh")).alias("greatest_voltage_kwh"),
    )
)

line_level_readings = (
    base.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.voltage_bucket",
        "s.greatest_voltage_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

joined_site = (
    line_level_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.voltage_bucket",
        "s.greatest_voltage_kwh",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_voltage",
        "s.reading_read_at",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

result = (
    joined_site
    .groupingSets(
        [["site_zone", "reading_quality"], ["site_zone"], []],
        "site_zone", "reading_quality",
    )
    .agg(F.min("reading_read_at").alias("first_reading_read_at"))
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("reading_quality").asc_nulls_last(),
    )
)
