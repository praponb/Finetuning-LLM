from pyspark.sql import functions as F

covered = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

joined = (
    covered.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

quality_stats = (
    joined
    .groupBy("quality")
    .agg(F.avg("site_installed_kw").alias("avg_site_installed_kw"))
)

vs_group = (
    joined.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.site_installed_kw") > F.col("g.avg_site_installed_kw"))
    .select(
        "s.*",
        "g.avg_site_installed_kw",
        F.round(F.col("s.site_installed_kw") - F.col("g.avg_site_installed_kw"), 2).alias("site_installed_kw_minus_avg"),
    )
)

result = (
    vs_group
    .cube("quality", "site_zone", "zone")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.stddev("kwh"), 2).alias("stddev_kwh"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        F.coalesce(F.col("site_zone"), F.lit("ALL")).alias("site_zone"),
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        "grouping_level",
        "stddev_kwh",
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("site_zone").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
    )
)
