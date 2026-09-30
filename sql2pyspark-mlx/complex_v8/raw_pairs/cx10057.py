from pyspark.sql import functions as F

recent_sites = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

without_matches = (
    recent_sites.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.read_at") < F.lit("2025-02-18 13:00:00").cast("timestamp")), "left_semi")
)

annotated = (
    without_matches
    .select(
        "*",
        F.length(F.trim(F.col("site_name"))).alias("site_name_len"),
        F.when(F.col("tariff").isin("FLAT", "TOU"), "group_a").when(F.col("tariff") == "EV", "group_b").otherwise("other").alias("tariff_group"),
    )
)

result = (
    annotated
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "commissioned_on",
        "site_name_len",
        F.concat_ws("|", F.col("site_name"), F.col("zone")).alias("site_name_zone_label"),
    )
    .orderBy(F.col("tariff").asc_nulls_last(), "site_name", "id")
    .limit(10)
)
