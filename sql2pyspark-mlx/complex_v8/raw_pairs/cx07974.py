from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone = Window.partitionBy("tariff", "zone")
w_tariff_by_installed_kw_fille = Window.partitionBy("tariff").orderBy(F.col("installed_kw_filled").desc(), "id")
w_tariff_by_installed_kw_fille_2 = Window.partitionBy("tariff").orderBy(F.col("installed_kw_filled").desc(), "id").rowsBetween(-3, Window.currentRow)
w_tariff_by_installed_kw_fille_3 = Window.partitionBy("tariff").orderBy(F.col("installed_kw_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_installed_kw_filled_t = Window.orderBy("installed_kw_filled")
w_all_by_tariff_zone_max_insta = Window.orderBy("tariff_zone_max_installed_kw", "id")
w_tariff = Window.partitionBy("tariff")
w_installed_kw_filled_dense_ra = Window.partitionBy("installed_kw_filled_dense_rank")
w_all_by_installed_kw_ties = Window.orderBy(F.col("installed_kw").desc())
w_all_by_rows_in_tariff = Window.orderBy("rows_in_tariff", "id")

scoped_sites = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-09-15").cast("date"), F.lit("2025-05-07").cast("date")))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("installed_kw"), F.lit(0.0)).alias("installed_kw_filled"),
    )
)

with_trends = (
    scoped_sites
    .select(
        "*",
        F.max("installed_kw").over(w_tariff_zone).alias("tariff_zone_max_installed_kw"),
        F.row_number().over(w_tariff_by_installed_kw_fille).alias("installed_kw_filled_seq"),
        F.sum("installed_kw").over(w_tariff_by_installed_kw_fille_2).alias("installed_kw_sum_4"),
        F.count(F.lit(1)).over(w_tariff_by_installed_kw_fille_3).alias("running_count"),
        F.round(F.avg("installed_kw").over(w_tariff_zone), 2).alias("tariff_zone_avg_installed_kw"),
    )
)

zone_stats = (
    with_trends
    .groupBy("zone")
    .agg(F.avg("installed_kw_sum_4").alias("avg_installed_kw_sum_4"))
)

outliers = (
    with_trends.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.installed_kw_sum_4") > F.col("g.avg_installed_kw_sum_4"))
    .select(
        "s.*",
        "g.avg_installed_kw_sum_4",
        F.round(F.col("s.installed_kw_sum_4") - F.col("g.avg_installed_kw_sum_4"), 2).alias("installed_kw_sum_4_minus_avg"),
    )
)

windowed = (
    outliers
    .select(
        "*",
        F.dense_rank().over(w_all_by_installed_kw_filled_t).alias("installed_kw_filled_dense_rank"),
        F.row_number().over(w_all_by_tariff_zone_max_insta).alias("tariff_zone_max_installed_kw_seq"),
        F.size(F.collect_set("zone").over(w_tariff)).alias("distinct_zone_in_tariff"),
        F.count(F.lit(1)).over(w_tariff).alias("rows_in_tariff"),
    )
)

windowed_2 = (
    windowed
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        "installed_kw_filled",
        "tariff_zone_max_installed_kw",
        "installed_kw_sum_4",
        "running_count",
        "tariff_zone_avg_installed_kw",
        "avg_installed_kw_sum_4",
        "installed_kw_sum_4_minus_avg",
        "installed_kw_filled_dense_rank",
        "tariff_zone_max_installed_kw_seq",
        "distinct_zone_in_tariff",
        "rows_in_tariff",
        F.round(F.col("running_count") / F.nullif(F.sum("running_count").over(w_installed_kw_filled_dense_ra), F.lit(0)), 4).alias("running_count_share_of_installed_kw_filled_dense"),
        F.round(F.col("installed_kw_sum_4") - F.avg("installed_kw_sum_4").over(w_installed_kw_filled_dense_ra), 2).alias("installed_kw_sum_4_vs_installed_kw_filled_dense_"),
        F.round(F.cume_dist().over(w_all_by_installed_kw_ties), 3).alias("installed_kw_cume_dist"),
        F.ntile(10).over(w_all_by_rows_in_tariff).alias("rows_in_tariff_decile"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw_filled",
        "tariff_zone_max_installed_kw",
        "installed_kw_sum_4",
        "running_count",
        "tariff_zone_avg_installed_kw",
        "avg_installed_kw_sum_4",
        "installed_kw_sum_4_minus_avg",
        "tariff_zone_max_installed_kw_seq",
        "rows_in_tariff",
        "running_count_share_of_installed_kw_filled_dense",
        "installed_kw_cume_dist",
        "rows_in_tariff_decile",
        F.coalesce(F.col("installed_kw_filled"), F.lit(0.0)).alias("installed_kw_filled_filled"),
    )
    .orderBy(F.col("tariff_zone_avg_installed_kw").desc())
)
