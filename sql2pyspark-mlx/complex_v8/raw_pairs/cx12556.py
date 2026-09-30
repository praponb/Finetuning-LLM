from pyspark.sql import functions as F

base = (
    df_meter_readings
    .filter((F.col("kwh") <= 86.0) & (F.col("voltage").between(221.4, 239.6)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

joined = (
    base.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        F.col("si.zone").alias("site_zone"),
    )
)

line_level_readings = (
    joined.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 224.9), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.site_zone",
        F.col("m.id").alias("reading_id"),
        F.col("m.read_at").alias("reading_read_at"),
        F.col("m.voltage").alias("reading_voltage"),
    )
)

result = (
    line_level_readings
    .filter(F.col("voltage") > 221.8)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "site_zone",
        "reading_id",
        F.abs(F.col("kwh") - 53.6).alias("kwh_distance"),
    )
    .orderBy(F.col("id").desc())
)
