from pyspark.sql import functions as F

recent_readings = (
    df_meter_readings
    .filter(F.col("voltage") > 219.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("voltage") - 231.7).alias("voltage_distance"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_details_site = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.kwh_per_voltage",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.zone").alias("site_zone"),
    )
)

with_activity = (
    with_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage").between(219.5, 235.8)), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "site_id",
        "voltage",
        "kwh_per_voltage",
        "site_commissioned_on",
    )
    .orderBy(F.col("voltage").asc(), "id")
    .limit(20)
)
