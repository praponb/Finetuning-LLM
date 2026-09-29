from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .filter((F.col("quality") == "OK") & (F.col("site_id") <= 8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.round(F.col("voltage") * 0.9, 2).alias("voltage_adj"),
    )
)

enriched = (
    scoped_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_clean",
        "s.days_since_read_at",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

joined = (
    enriched.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone") == "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.days_since_read_at",
        "s.site_zone",
        "s.site_installed_kw",
        F.col("si.zone").alias("site_zone_2"),
        F.col("si.installed_kw").alias("site_installed_kw_2"),
    )
)

result = (
    joined
    .rollup("quality", "site_zone", "site_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("days_since_read_at"), 2).alias("avg_days_since_read_at"),
        F.array_sort(F.collect_set("site_zone")).alias("site_zone_list"),
        F.count(F.when(~F.col("site_zone_2").isin("Z3", "Z4"), 1)).alias("n_site_zone_2_not_in_z3_z4"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
        F.col("site_id").asc_nulls_last(),
    )
)
