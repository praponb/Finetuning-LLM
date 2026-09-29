from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter((F.to_date(F.col("read_at")) < F.lit("2025-02-12").cast("date")) & (F.col("kwh") >= 52.4))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 63.4).alias("kwh_distance"),
        F.year(F.col("read_at")).alias("read_at_year"),
    )
)

qualified = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.hour(F.col("m.read_at")).between(10, 13)) & (F.col("m.quality").isin("EST", "BAD")), "left_semi")
)

with_details = (
    qualified.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.read_at_year",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

result = (
    with_details
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_distance",
        "read_at_year",
        "site_zone",
        "site_name",
    )
    .orderBy(F.col("voltage").desc())
)
