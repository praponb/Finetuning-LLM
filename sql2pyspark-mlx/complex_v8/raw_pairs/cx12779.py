from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone = Window.partitionBy("tariff", "zone")
w_zone_by_installed_kw_ties = Window.partitionBy("zone").orderBy("installed_kw")
w_zone_by_installed_kw_full = Window.partitionBy("zone").orderBy("installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_installed_kw_vs_tarif = Window.orderBy(F.col("installed_kw_vs_tariff_zone_avg").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_installed_kw_vs_tarif_2 = Window.orderBy(F.col("installed_kw_vs_tariff_zone_avg").desc_nulls_first())

prepared_sites = (
    df_sites
    .filter(F.col("zone") == "Z4")
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        F.date_format(F.col("commissioned_on"), "yyyy-MM").alias("commissioned_on_label"),
    )
)

windowed = (
    prepared_sites
    .select(
        "id",
        "zone",
        "tariff",
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_tariff_zone), 2).alias("installed_kw_vs_tariff_zone_avg"),
        F.round(F.percent_rank().over(w_zone_by_installed_kw_ties), 3).alias("installed_kw_percent_rank"),
        F.dense_rank().over(w_zone_by_installed_kw_ties).alias("installed_kw_dense_rank"),
        F.round(F.col("installed_kw") / F.nullif(F.sum("installed_kw").over(w_tariff_zone), F.lit(0)), 4).alias("installed_kw_share_of_tariff_zone"),
        F.nth_value("installed_kw", 2).over(w_zone_by_installed_kw_full).alias("second_installed_kw"),
    )
)

leaders = (
    windowed
    .filter(F.col("installed_kw_dense_rank") <= 2)
    .drop("installed_kw_dense_rank")
)

screened = (
    leaders
    .crossJoin(leaders.agg(F.avg("installed_kw_percent_rank").alias("overall_avg_installed_kw_percent_rank")))
    .filter(F.col("installed_kw_percent_rank") > F.col("overall_avg_installed_kw_percent_rank"))
    .drop("overall_avg_installed_kw_percent_rank")
)

ranked = (
    screened
    .select(
        "*",
        F.nth_value("second_installed_kw", 2).over(w_all_by_installed_kw_vs_tarif).alias("second_second_installed_kw"),
        F.dense_rank().over(w_all_by_installed_kw_vs_tarif_2).alias("installed_kw_vs_tariff_zone_avg_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("second_second_installed_kw").between(66.5, 386.6))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw_vs_tariff_zone_avg",
        "installed_kw_percent_rank",
        "installed_kw_share_of_tariff_zone",
        "second_installed_kw",
        "second_second_installed_kw",
        "installed_kw_vs_tariff_zone_avg_dense_rank",
    )
    .orderBy(
        "installed_kw_vs_tariff_zone_avg",
        F.col("installed_kw_vs_tariff_zone_avg_dense_rank").desc(),
    )
)
