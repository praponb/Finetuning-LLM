from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("zone").orderBy("tariff_len", "id")

scoped = (
    df_sites
    .filter(F.col("site_name").isNotNull())
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.abs(F.col("installed_kw") - 141.6).alias("installed_kw_distance"),
        F.length(F.trim(F.col("tariff"))).alias("tariff_len"),
    )
)

zone_stats = (
    scoped
    .groupBy("zone")
    .agg(F.percentile_approx("tariff_len", 0.5).alias("median_tariff_len"))
)

compared = (
    scoped.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.tariff_len") >= F.col("g.median_tariff_len"))
    .select(
        "s.*",
        "g.median_tariff_len",
        F.round(F.col("s.tariff_len") - F.col("g.median_tariff_len"), 2).alias("tariff_len_minus_median"),
    )
)

latest = (
    compared
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

tagged = (
    latest
    .filter(F.col("installed_kw_distance") < 450.0)
    .select(
        "*",
        F.round(F.col("installed_kw_distance") * 0.9, 2).alias("installed_kw_distance_adj"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

result = (
    tagged
    .filter(F.col("median_tariff_len").between(5.6, 10.4))
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "commissioned_on",
        "installed_kw_distance",
        "median_tariff_len",
        "tariff_len_minus_median",
        "installed_kw_distance_adj",
        (F.floor(F.col("installed_kw_bucket") / 100) * 100).alias("installed_kw_bucket_bucket"),
    )
)
