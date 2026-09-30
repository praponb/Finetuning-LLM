from pyspark.sql import functions as F

valid_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 08:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
        (F.col("voltage") - F.col("kwh")).alias("voltage_minus_kwh"),
    )
)

with_children = (
    valid_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        F.col("m.id").alias("reading_id"),
        F.col("m.quality").alias("reading_quality"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

screened = (
    with_children.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isin("OK", "EST")) & (F.col("m.read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_semi")
)

enriched_reading = (
    screened.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at",
        "s.reading_voltage",
        F.col("m.site_id").alias("reading_site_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

enriched = (
    enriched_reading.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.reading_site_id"), "inner")
    .select("s.*", "si.site_name")
)

with_children_2 = (
    enriched.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.voltage_minus_kwh",
        "s.reading_id",
        "s.reading_quality",
        "s.reading_read_at",
        "s.reading_voltage",
        "s.reading_site_id",
        "s.reading_kwh",
        "s.site_zone",
        "s.site_commissioned_on",
        "s.site_name",
        F.col("m.id").alias("reading_id_2"),
        F.col("m.read_at").alias("reading_read_at_2"),
        F.col("m.voltage").alias("reading_voltage_2"),
        F.col("m.quality").alias("reading_quality_2"),
    )
)

result = (
    with_children_2
    .select(
        "id",
        "site_id",
        "voltage_minus_kwh",
        "reading_id",
        "reading_read_at",
        "reading_site_id",
        "reading_kwh",
        "site_zone",
        "site_commissioned_on",
        "site_name",
        "reading_id_2",
        "reading_read_at_2",
        "reading_voltage_2",
    )
    .orderBy(
        F.col("site_name").desc(),
        "id",
        "reading_id",
        "reading_id_2",
    )
    .limit(5)
)
