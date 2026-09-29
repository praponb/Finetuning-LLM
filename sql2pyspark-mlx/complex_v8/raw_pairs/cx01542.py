from pyspark.sql import functions as F

prepared_readings = (
    df_meter_readings
    .filter((F.hour(F.col("read_at")).between(6, 10)) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

filtered = (
    prepared_readings
    .crossJoin(prepared_readings.agg(F.avg("kwh").alias("overall_avg_kwh")))
    .filter(F.col("kwh") > F.col("overall_avg_kwh"))
    .drop("overall_avg_kwh")
)

with_activity = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-19 13:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("kwh").between(30.2, 110.5))
)

with_parent = (
    with_activity.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .select(
        "s.*",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "site_commissioned_on",
        "site_zone",
        F.concat_ws(" / ", F.col("site_zone"), F.col("quality")).alias("site_zone_quality_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
    )
    .orderBy(F.col("kwh").desc(), "site_commissioned_on")
)
