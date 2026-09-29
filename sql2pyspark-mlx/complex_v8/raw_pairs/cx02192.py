from pyspark.sql import functions as F

scoped = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-05-10").cast("date"))) & (F.col("tariff") != "FLAT"))
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality") == "OK") & (F.col("m.kwh").between(10.9, 113.0)), "left_anti")
)

tariff_stats = (
    without_matches
    .groupBy("tariff")
    .agg(F.percentile_approx("installed_kw", 0.5).alias("median_installed_kw"))
)

vs_group = (
    without_matches.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.installed_kw") >= F.col("g.median_installed_kw"))
    .select(
        "s.*",
        "g.median_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.median_installed_kw"), 2).alias("installed_kw_minus_median"),
    )
)

qualified = (
    vs_group.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .filter(F.col("installed_kw") > 96.9)
    .select(
        "id",
        "site_name",
        "tariff",
        "installed_kw",
        F.round(F.col("installed_kw") / F.nullif(F.col("median_installed_kw"), F.lit(0)), 3).alias("installed_kw_per_median_installed_kw"),
    )
    .orderBy("id", F.col("installed_kw").asc_nulls_last())
)
