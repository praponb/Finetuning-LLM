from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone_by_min_installed = Window.partitionBy("tariff", "zone").orderBy(F.col("min_installed_kw_adj").desc_nulls_first(), "zone", "tariff")
w_all_by_min_installed_kw_adj = Window.orderBy("min_installed_kw_adj", "zone", "tariff").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_min_installed_kw_adj_2 = Window.orderBy("min_installed_kw_adj", "zone", "tariff").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_sites = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
        F.round(F.col("installed_kw") * 0.25, 2).alias("installed_kw_adj"),
    )
)

per_group = (
    cleaned_sites
    .groupBy("zone", "tariff")
    .agg(
        F.array_sort(F.collect_set("tariff")).alias("tariff_list"),
        F.min("installed_kw_adj").alias("min_installed_kw_adj"),
    )
)

with_trends = (
    per_group
    .select(
        "zone",
        "tariff",
        F.round(F.percent_rank().over(w_tariff_zone_by_min_installed), 3).alias("min_installed_kw_adj_percent_rank"),
        F.count(F.lit(1)).over(w_all_by_min_installed_kw_adj).alias("running_count"),
        F.nth_value("min_installed_kw_adj", 2).over(w_all_by_min_installed_kw_adj_2).alias("second_min_installed_kw_adj"),
    )
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("min_installed_kw_adj_percent_rank").alias("overall_avg_min_installed_kw_adj_percent_rank")))
    .filter(F.col("min_installed_kw_adj_percent_rank") > F.col("overall_avg_min_installed_kw_adj_percent_rank"))
    .drop("overall_avg_min_installed_kw_adj_percent_rank")
)

result = (
    screened
    .groupBy("tariff")
    .pivot("zone", ["Z3", "Z2", "Z4"])
    .agg(F.max("min_installed_kw_adj_percent_rank"))
    .orderBy("tariff")
)
