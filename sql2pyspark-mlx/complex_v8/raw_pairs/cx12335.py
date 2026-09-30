from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_commissioned_on_runni = Window.orderBy(F.col("commissioned_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_commissioned_on = Window.orderBy(F.col("commissioned_on").asc_nulls_last(), "id")
w_zone = Window.partitionBy("zone")
w_all_by_commissioned_on_full = Window.orderBy(F.col("commissioned_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_commissioned_on_quintile = Window.partitionBy("commissioned_on_quintile")
w_all_by_commissioned_on_quint = Window.orderBy("commissioned_on_quintile", "id")

cleaned = (
    df_sites
    .filter(F.col("zone").isNotNull())
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "zone",
        "installed_kw",
        "commissioned_on",
        "tariff_or_unknown",
        F.first("installed_kw", ignorenulls=True).over(w_all_by_commissioned_on_runni).alias("first_known_installed_kw"),
        F.ntile(5).over(w_all_by_commissioned_on).alias("commissioned_on_quintile"),
        F.sum("installed_kw").over(w_zone).alias("zone_sum_installed_kw"),
        F.nth_value("commissioned_on", 2).over(w_all_by_commissioned_on_full).alias("second_commissioned_on"),
        F.dense_rank().over(w_all_by_commissioned_on).alias("commissioned_on_dense_rank"),
    )
)

leaders = ranked.filter(F.col("commissioned_on_dense_rank") <= 5)

with_windows = (
    leaders
    .select(
        "*",
        F.round(F.avg("installed_kw").over(w_commissioned_on_quintile), 2).alias("commissioned_on_quintile_avg_installed_kw"),
        F.lead("commissioned_on_quintile").over(w_all_by_commissioned_on_quint).alias("next_commissioned_on_quintile"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["commissioned_on_dense_rank", "tariff_or_unknown", "commissioned_on_quintile"], ["commissioned_on_dense_rank"], ["tariff_or_unknown"]],
        "commissioned_on_dense_rank", "tariff_or_unknown", "commissioned_on_quintile",
    )
    .agg(
        F.grouping("commissioned_on_dense_rank").alias("is_total_commissioned_on_dense_rank"),
        F.array_sort(F.collect_set("zone")).alias("zone_list"),
    )
    .orderBy(
        F.col("commissioned_on_dense_rank").asc_nulls_last(),
        F.col("tariff_or_unknown").asc_nulls_last(),
        F.col("commissioned_on_quintile").asc_nulls_last(),
    )
)
