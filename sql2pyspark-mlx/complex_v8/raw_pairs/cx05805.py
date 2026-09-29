from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_by_max_installed_kw_min = Window.partitionBy("zone").orderBy("max_installed_kw_minus_avg")
w_all_by_max_installed_kw_full = Window.orderBy("max_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone = Window.partitionBy("zone")
w_tariff_by_max_installed_kw_m = Window.partitionBy("tariff").orderBy("max_installed_kw_minus_avg_rank", "id")
w_all_by_installed_kw_minus_ma = Window.orderBy(F.col("installed_kw_minus_max").desc(), "id")
w_all_by_first_installed_kw_mi = Window.orderBy(F.col("first_installed_kw_minus_max").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev_installed_kw_run = Window.orderBy("prev_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_first_installed_kw_mi_2 = Window.orderBy(F.col("first_installed_kw_minus_max").desc_nulls_first())

scoped_sites = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
    )
)

tariff_stats = (
    scoped_sites
    .groupBy("tariff")
    .agg(F.max("installed_kw").alias("max_installed_kw"))
)

above_average = (
    scoped_sites.alias("s")
    .join(tariff_stats.alias("g"), F.col("g.tariff") == F.col("s.tariff"), "inner")
    .filter(F.col("s.installed_kw") == F.col("g.max_installed_kw"))
    .select(
        "s.*",
        "g.max_installed_kw",
        F.round(F.col("s.installed_kw") - F.col("g.max_installed_kw"), 2).alias("installed_kw_minus_max"),
    )
)

zone_stats = (
    above_average
    .groupBy("zone")
    .agg(F.avg("max_installed_kw").alias("avg_max_installed_kw"))
)

outliers = (
    above_average.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.max_installed_kw") > F.col("g.avg_max_installed_kw"))
    .select(
        "s.*",
        "g.avg_max_installed_kw",
        F.round(F.col("s.max_installed_kw") - F.col("g.avg_max_installed_kw"), 2).alias("max_installed_kw_minus_avg"),
    )
)

metrics = (
    outliers
    .select(
        "*",
        F.rank().over(w_zone_by_max_installed_kw_min).alias("max_installed_kw_minus_avg_rank"),
        F.first("installed_kw_minus_max").over(w_all_by_max_installed_kw_full).alias("first_installed_kw_minus_max"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.read_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("max_installed_kw") > 203.0)
)

sequenced = (
    with_activity
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "max_installed_kw",
        "installed_kw_minus_max",
        "avg_max_installed_kw",
        "max_installed_kw_minus_avg",
        "max_installed_kw_minus_avg_rank",
        "first_installed_kw_minus_max",
        F.round(F.col("installed_kw") - F.avg("installed_kw").over(w_zone), 2).alias("installed_kw_vs_zone_avg"),
        F.lag("installed_kw").over(w_tariff_by_max_installed_kw_m).alias("prev_installed_kw"),
        F.count(F.lit(1)).over(w_zone).alias("rows_in_zone"),
        (F.col("max_installed_kw_minus_avg") - F.lag("max_installed_kw_minus_avg").over(w_all_by_installed_kw_minus_ma)).alias("max_installed_kw_minus_avg_change"),
    )
)

with_trends = (
    sequenced
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "max_installed_kw",
        "avg_max_installed_kw",
        "max_installed_kw_minus_avg",
        "max_installed_kw_minus_avg_rank",
        "first_installed_kw_minus_max",
        "installed_kw_vs_zone_avg",
        "prev_installed_kw",
        "rows_in_zone",
        "max_installed_kw_minus_avg_change",
        F.first("rows_in_zone", ignorenulls=True).over(w_all_by_first_installed_kw_mi).alias("first_known_rows_in_zone"),
        F.last("first_installed_kw_minus_max", ignorenulls=True).over(w_all_by_prev_installed_kw_run).alias("last_known_first_installed_kw_minus_max"),
        F.rank().over(w_all_by_first_installed_kw_mi_2).alias("first_installed_kw_minus_max_rank"),
        F.last("installed_kw_vs_zone_avg", ignorenulls=True).over(w_all_by_prev_installed_kw_run).alias("last_known_installed_kw_vs_zone_avg"),
    )
)

result = (
    with_trends
    .select("id", "first_installed_kw_minus_max", "installed_kw")
    .unpivot(
        ids=["id"],
        values=["first_installed_kw_minus_max", "installed_kw"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .orderBy("id", "metric_name")
)
