from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_zone_in_z2_z4_by_distinct = Window.partitionBy("n_zone_in_z2_z4").orderBy(F.col("distinct_tariff_or_unknowns").desc(), "tariff_or_unknown").rowsBetween(-6, Window.currentRow)
w_tariff_or_unknown = Window.partitionBy("tariff_or_unknown")
w_all_by_pct_upper_trim_site_n = Window.orderBy("pct_upper_trim_site_name_like_m", "tariff_or_unknown").rowsBetween(-1, Window.currentRow)
w_all_by_pct_upper_trim_site_n_2 = Window.orderBy("pct_upper_trim_site_name_like_m", "tariff_or_unknown").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_pct_upper_trim_site_n_3 = Window.orderBy("pct_upper_trim_site_name_like_m", "tariff_or_unknown")

prepared = (
    df_sites
    .filter(F.col("installed_kw") < 373.3)
    .select(
        "id",
        "site_name",
        "zone",
        "commissioned_on",
        F.coalesce(F.col("tariff"), F.lit("UNKNOWN")).alias("tariff_or_unknown"),
    )
)

grouped = (
    prepared
    .filter(F.col("commissioned_on") >= F.lit("2024-09-15").cast("date"))
    .groupBy("tariff_or_unknown")
    .agg(
        F.count(F.when(F.col("zone") != "Z4", 1)).alias("n_zone_z4"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("site_name"))).like("M%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_site_name_like_m"),
        F.sum(F.when(F.col("zone").isin("Z2", "Z4"), 1).otherwise(0)).alias("n_zone_in_z2_z4"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.countDistinct("tariff_or_unknown").alias("distinct_tariff_or_unknowns"),
    )
)

metrics = (
    grouped
    .select(
        "tariff_or_unknown",
        "n_zone_z4",
        "pct_upper_trim_site_name_like_m",
        "n_zone_in_z2_z4",
        "distinct_tariff_or_unknowns",
        F.round(F.avg("n_zone_in_z2_z4").over(w_n_zone_in_z2_z4_by_distinct), 2).alias("n_zone_in_z2_z4_avg_7"),
        F.count(F.lit(1)).over(w_tariff_or_unknown).alias("rows_in_tariff_or_unknown"),
    )
)

metrics_2 = (
    metrics
    .select(
        "*",
        F.sum("rows_in_tariff_or_unknown").over(w_all_by_pct_upper_trim_site_n).alias("rows_in_tariff_or_unknown_sum_2"),
        F.min("n_zone_in_z2_z4").over(w_all_by_pct_upper_trim_site_n_2).alias("min_n_zone_in_z2_z4_so_far"),
        F.lag("n_zone_in_z2_z4_avg_7").over(w_all_by_pct_upper_trim_site_n_3).alias("prev_n_zone_in_z2_z4_avg_7"),
        (F.col("n_zone_z4") - F.lag("n_zone_z4").over(w_all_by_pct_upper_trim_site_n_3)).alias("n_zone_z4_change"),
    )
)

result = (
    metrics_2
    .select(
        "tariff_or_unknown",
        "n_zone_z4",
        "pct_upper_trim_site_name_like_m",
        "n_zone_in_z2_z4",
        "distinct_tariff_or_unknowns",
        "n_zone_in_z2_z4_avg_7",
        "rows_in_tariff_or_unknown",
        "min_n_zone_in_z2_z4_so_far",
        "prev_n_zone_in_z2_z4_avg_7",
    )
    .orderBy(
        F.col("rows_in_tariff_or_unknown").asc_nulls_last(),
        F.col("tariff_or_unknown").asc_nulls_last(),
    )
)
