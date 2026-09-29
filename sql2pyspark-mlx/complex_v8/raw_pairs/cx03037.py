from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter((F.col("zone").isin("Z4", "Z2")) & (F.upper(F.trim(F.col("site_name"))).like("H%")))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("site_name")), " ", "_")).alias("site_name_slug"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.kwh") < 106.5) & (F.col("m.read_at") < F.lit("2025-02-18 07:00:00").cast("timestamp")), "left_semi")
)

result = (
    without_matches
    .select(
        "id",
        "zone",
        "commissioned_on",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)
