from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_commissioned_on_plus_6m = Window.partitionBy("zone", F.year("commissioned_on_plus_6m")).orderBy("installed_kw")
w_zone_tariff = Window.partitionBy("zone", "tariff")

scoped_sites = (
    df_sites
    .filter((F.col("site_name").isNotNull()) | (F.col("installed_kw") > 93.3))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.add_months(F.col("commissioned_on"), 6).alias("commissioned_on_plus_6m"),
    )
)

with_trends = (
    scoped_sites
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.rank().over(w_zone_commissioned_on_plus_6m).alias("installed_kw_rank"),
        F.round(F.percent_rank().over(w_zone_commissioned_on_plus_6m), 3).alias("installed_kw_percent_rank"),
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_zone_tariff), 2).alias("installed_kw_vs_zone_tariff_avg"),
    )
)

tariff_stats = (
    with_trends
    .groupBy("tariff")
    .agg(F.percentile_approx("installed_kw_vs_zone_tariff_avg", 0.5).alias("median_installed_kw_vs_zone_tariff_avg"))
)

compared = (
    with_trends.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.installed_kw_vs_zone_tariff_avg") >= F.col("g.median_installed_kw_vs_zone_tariff_avg"))
    .select(
        "s.*",
        "g.median_installed_kw_vs_zone_tariff_avg",
        F.round(F.col("s.installed_kw_vs_zone_tariff_avg") - F.col("g.median_installed_kw_vs_zone_tariff_avg"), 2).alias("installed_kw_vs_zone_tariff_avg_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "installed_kw_rank",
        "installed_kw_percent_rank",
        "installed_kw_vs_zone_tariff_avg",
        "median_installed_kw_vs_zone_tariff_avg",
        "installed_kw_vs_zone_tariff_avg_minus_median",
        F.date_format(F.col("commissioned_on"), "MMM yyyy").alias("commissioned_on_label"),
    )
    .orderBy(F.col("tariff").desc())
)
