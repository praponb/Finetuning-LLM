from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_installed_kw_bucket = Window.partitionBy("zone").orderBy(F.col("installed_kw_bucket_tariff_in_ev_tou").desc(), "zone", "tariff").rowsBetween(-3, Window.currentRow)
w_zone = Window.partitionBy("zone")
w_zone_by_installed_kw_bucket_2 = Window.partitionBy("zone").orderBy(F.col("installed_kw_bucket_tariff_in_ev_tou").desc())
w_zone_by_installed_kw_bucket_3 = Window.partitionBy("zone").orderBy(F.col("installed_kw_bucket_tariff_in_ev_tou").desc(), "zone", "tariff")

base = (
    df_sites
    .filter(F.length(F.trim(F.col("site_name"))) >= 4)
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.abs(F.col("installed_kw") - 373.3).alias("installed_kw_distance"),
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
    )
)

per_group = (
    base
    .groupBy("zone", "tariff")
    .agg(
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.sum(F.when(F.col("tariff").isin("EV", "TOU"), F.col("installed_kw_bucket"))).alias("installed_kw_bucket_tariff_in_ev_tou"),
        F.round(F.sum("installed_kw_distance") / F.nullif(F.sum("installed_kw_bucket"), F.lit(0)), 3).alias("installed_kw_distance_to_installed_kw_bucket_rat"),
        F.round(F.stddev("installed_kw"), 2).alias("stddev_installed_kw"),
    )
)

with_windows = (
    per_group
    .select(
        "*",
        F.round(F.avg("stddev_installed_kw").over(w_zone_by_installed_kw_bucket), 2).alias("stddev_installed_kw_avg_4"),
        F.round(F.col("stddev_installed_kw") - F.avg("stddev_installed_kw").over(w_zone), 2).alias("stddev_installed_kw_vs_zone_avg"),
        F.rank().over(w_zone_by_installed_kw_bucket_2).alias("installed_kw_bucket_tariff_in_ev_tou_rank"),
        F.row_number().over(w_zone_by_installed_kw_bucket_3).alias("rn"),
        F.min("stddev_installed_kw").over(w_zone).alias("zone_min_stddev_installed_kw"),
        F.max("stddev_installed_kw").over(w_zone).alias("zone_max_stddev_installed_kw"),
    )
)

result = (
    with_windows
    .select(
        "zone",
        "tariff",
        "stddev_installed_kw_avg_4",
        "installed_kw_distance_to_installed_kw_bucket_rat",
        "zone_min_stddev_installed_kw",
    )
    .unpivot(
        ids=["zone", "tariff"],
        values=["stddev_installed_kw_avg_4", "installed_kw_distance_to_installed_kw_bucket_rat", "zone_min_stddev_installed_kw"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
