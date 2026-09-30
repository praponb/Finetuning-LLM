from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("voltage") <= 235.0) & (F.col("kwh") <= 81.1))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

with_children = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_label",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.quality").alias("reading_quality"),
    )
)

with_activity = (
    with_children.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

enriched_site = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff") != "FLAT"), "inner")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.read_at_label",
        "s.reading_id",
        F.col("si.tariff").alias("site_tariff"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

result = (
    enriched_site
    .select(
        "id",
        "site_id",
        "read_at",
        "reading_id",
        "site_tariff",
        "reading_voltage",
        "reading_read_at_2",
    )
    .orderBy(F.col("site_id").desc())
)
