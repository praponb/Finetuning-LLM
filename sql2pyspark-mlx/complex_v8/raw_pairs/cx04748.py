from pyspark.sql import functions as F

filtered_readings = (
    df_meter_readings
    .filter(F.col("voltage") >= 223.0)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        (F.col("read_at") + F.expr("INTERVAL 30 MINUTES")).alias("read_at_plus_30m"),
    )
)

derived = (
    filtered_readings
    .filter(F.month(F.col("read_at_plus_30m")).isin(1, 2))
    .select(
        "*",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean_2"),
    )
)

with_details_site = (
    derived.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.read_at_plus_30m",
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

without_matches = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (~F.col("si.zone").isin("Z2", "Z3")) & (F.col("si.installed_kw") > 96.9), "left_anti")
)

result = (
    without_matches
    .rollup("site_zone", "quality", "site_id")
    .agg(
        F.grouping("site_zone").alias("is_total_site_zone"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("site_zone") == "Z2", 1).otherwise(0)).alias("n_site_zone_z2"),
    )
    .select(
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "site_id",
        "is_total_site_zone",
        "distinct_ids",
        "n_site_zone_z2",
    )
    .orderBy(
        F.col("site_zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
