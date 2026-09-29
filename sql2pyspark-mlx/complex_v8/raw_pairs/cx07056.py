from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff_zone_by_installed_kw = Window.partitionBy("tariff", "zone").orderBy(F.col("installed_kw_bucket").asc_nulls_last(), "id")
w_zone_by_installed_kw_moving = Window.partitionBy("zone").orderBy(F.col("installed_kw").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_zone_by_installed_kw_ties = Window.partitionBy("zone").orderBy(F.col("installed_kw").asc_nulls_last())
w_tariff_zone_by_prev_installe = Window.partitionBy("tariff", "zone").orderBy(F.col("prev_installed_kw").asc_nulls_last(), "id")
w_tariff_zone_by_prev_installe_2 = Window.partitionBy("tariff", "zone").orderBy(F.col("prev_installed_kw").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone = Window.partitionBy("zone")

valid = (
    df_sites
    .select(
        "id",
        "site_name",
        "zone",
        "tariff",
        "installed_kw",
        F.upper(F.trim(F.col("zone"))).alias("zone_clean"),
    )
)

labeled = (
    valid
    .filter(F.col("tariff").isNotNull())
    .select(
        "*",
        (F.floor(F.col("installed_kw") / 100) * 100).alias("installed_kw_bucket"),
        F.round(F.col("installed_kw") * 0.9, 2).alias("installed_kw_adj"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.lag("installed_kw").over(w_tariff_zone_by_installed_kw).alias("prev_installed_kw"),
        F.max("installed_kw_adj").over(w_zone_by_installed_kw_moving).alias("installed_kw_adj_max_4"),
        F.round(F.percent_rank().over(w_zone_by_installed_kw_ties), 3).alias("installed_kw_percent_rank"),
        F.round(F.cume_dist().over(w_zone_by_installed_kw_ties), 3).alias("installed_kw_cume_dist"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.substring(F.trim(F.col("site_name")), 1, 2).alias("site_name_prefix"),
    )
)

with_trends = (
    derived
    .select(
        "id",
        "site_name",
        "zone",
        "installed_kw",
        "zone_clean",
        "prev_installed_kw",
        "installed_kw_adj_max_4",
        "installed_kw_percent_rank",
        "site_name_prefix",
        F.lag("installed_kw_percent_rank", 2).over(w_tariff_zone_by_prev_installe).alias("prev2_installed_kw_percent_rank"),
        F.nth_value("prev_installed_kw", 2).over(w_tariff_zone_by_prev_installe_2).alias("second_prev_installed_kw"),
        F.sum("installed_kw_percent_rank").over(w_zone).alias("zone_sum_installed_kw_percent_rank"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.count("*").alias("reading_count"))
)

profiled = (
    with_trends.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.id"), "left_semi")
)

result = (
    filtered
    .filter(F.col("prev2_installed_kw_percent_rank") <= 0.6)
    .select(
        "id",
        "site_name",
        "installed_kw",
        "installed_kw_percent_rank",
        "prev2_installed_kw_percent_rank",
        "second_prev_installed_kw",
        "zone_sum_installed_kw_percent_rank",
        "reading_count",
        F.round(F.col("second_prev_installed_kw") * 1.5, 2).alias("second_prev_installed_kw_adj"),
    )
    .orderBy("second_prev_installed_kw_adj")
)
