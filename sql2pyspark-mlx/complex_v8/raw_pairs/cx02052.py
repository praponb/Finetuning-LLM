from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_tariff = Window.partitionBy("quality", "site_tariff")
w_site_tariff_quality_by_kwh = Window.partitionBy("site_tariff", "quality").orderBy("kwh", "id")
w_site_tariff_quality_by_kwh_f = Window.partitionBy("site_tariff", "quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_full = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").desc_nulls_first(), "id")
w_site_tariff_quality_by_kwh_t = Window.partitionBy("site_tariff", "quality").orderBy("kwh")
w_site_id_quality_by_kwh_prior = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_site_id_quality_by_kwh_runni = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_tercile_distinct_site_id = Window.partitionBy("kwh_tercile", "distinct_site_id_in_quality_site_tariff").orderBy("kwh", "id")
w_kwh_decile_2 = Window.partitionBy("kwh_decile_2")
w_distinct_site_id_in_quality = Window.partitionBy("distinct_site_id_in_quality_site_tariff", "site_id").orderBy("kwh_decile_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_kwh_quartile_by_last_kwh_to = Window.partitionBy("kwh_quartile").orderBy("last_kwh_to_kwh_share_of_quality_site_tariff_rat")
w_distinct_site_tariffs_by_las = Window.partitionBy("distinct_site_tariffs").orderBy(F.col("last_kwh_to_kwh_share_of_quality_site_tariff_rat").desc_nulls_first(), "kwh_quartile", "kwh_decile_2", "quality")
w_kwh_quartile_by_last_kwh_to_2 = Window.partitionBy("kwh_quartile").orderBy("last_kwh_to_kwh_share_of_quality_site_tariff_rat", "kwh_quartile", "kwh_decile_2", "quality")

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") < 86.0) & (F.month(F.col("read_at")).isin(1, 2)))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

combined_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        F.col("si.tariff").alias("site_tariff"),
        "si.site_name",
    )
)

with_windows = (
    combined_details_site
    .select(
        "*",
        F.round(F.avg("kwh").over(w_quality_site_tariff), 2).alias("quality_site_tariff_avg_kwh"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_tariff), F.lit(0)), 4).alias("kwh_share_of_quality_site_tariff"),
        F.ntile(3).over(w_site_tariff_quality_by_kwh).alias("kwh_tercile"),
        F.last("kwh").over(w_site_tariff_quality_by_kwh_f).alias("last_kwh"),
        F.nth_value("kwh", 2).over(w_quality_site_id_by_kwh_full).alias("second_kwh"),
        F.ntile(10).over(w_quality_site_id_by_kwh).alias("kwh_decile"),
        F.lag("kwh").over(w_site_tariff_quality_by_kwh).alias("prev_kwh"),
        F.sum("kwh").over(w_quality_site_tariff).alias("quality_site_tariff_sum_kwh"),
        F.round(F.cume_dist().over(w_site_tariff_quality_by_kwh_t), 3).alias("kwh_cume_dist"),
        F.round(F.avg("kwh").over(w_site_id_quality_by_kwh_prior), 2).alias("kwh_avg_2_prior"),
        F.count(F.lit(1)).over(w_site_id_quality_by_kwh_runni).alias("running_count"),
        F.size(F.collect_set("site_id").over(w_quality_site_tariff)).alias("distinct_site_id_in_quality_site_tariff"),
        F.ntile(10).over(w_site_tariff_quality_by_kwh).alias("kwh_decile_2"),
        F.min("kwh").over(w_site_id_quality_by_kwh_runni).alias("min_kwh_so_far"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "site_tariff",
        "site_name",
        "quality_site_tariff_avg_kwh",
        "kwh_share_of_quality_site_tariff",
        "kwh_tercile",
        "last_kwh",
        "kwh_decile",
        "prev_kwh",
        "kwh_cume_dist",
        "kwh_avg_2_prior",
        "running_count",
        "distinct_site_id_in_quality_site_tariff",
        "kwh_decile_2",
        "min_kwh_so_far",
        F.ntile(4).over(w_kwh_tercile_distinct_site_id).alias("kwh_quartile"),
        F.count(F.lit(1)).over(w_kwh_decile_2).alias("rows_in_kwh_decile_2"),
        F.first("running_count").over(w_distinct_site_id_in_quality).alias("first_running_count"),
    )
)

grouped = (
    metrics
    .groupBy("kwh_quartile", "kwh_decile_2", "quality")
    .agg(
        F.round(F.sum("last_kwh") / F.nullif(F.sum("kwh_share_of_quality_site_tariff"), F.lit(0)), 3).alias("last_kwh_to_kwh_share_of_quality_site_tariff_rat"),
        F.round(F.sum("prev_kwh") / F.nullif(F.sum("distinct_site_id_in_quality_site_tariff"), F.lit(0)), 3).alias("prev_kwh_to_distinct_site_id_in_quality_site_tar"),
        F.countDistinct("site_tariff").alias("distinct_site_tariffs"),
        F.array_sort(F.collect_set("site_name")).alias("site_name_list"),
        F.max("prev_kwh").alias("max_prev_kwh"),
    )
)

with_trends = (
    grouped
    .select(
        "*",
        F.rank().over(w_kwh_quartile_by_last_kwh_to).alias("last_kwh_to_kwh_share_of_quality_site_tariff_rat_2"),
        (F.col("distinct_site_tariffs") - F.lag("distinct_site_tariffs").over(w_distinct_site_tariffs_by_las)).alias("distinct_site_tariffs_change"),
        F.lead("distinct_site_tariffs", 3).over(w_kwh_quartile_by_last_kwh_to_2).alias("next3_distinct_site_tariffs"),
        F.rank().over(w_distinct_site_tariffs_by_las).alias("last_kwh_to_kwh_share_of_quality_site_tariff_rat_3"),
    )
)

result = (
    with_trends
    .filter(F.col("prev_kwh_to_distinct_site_id_in_quality_site_tar") >= 4.0)
    .select(
        "kwh_quartile",
        "distinct_site_tariffs",
        "site_name_list",
        "max_prev_kwh",
        "last_kwh_to_kwh_share_of_quality_site_tariff_rat_2",
        "distinct_site_tariffs_change",
        "next3_distinct_site_tariffs",
        F.abs(F.col("kwh_quartile") - 6).alias("kwh_quartile_distance"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("distinct_site_tariffs_change").asc(), "distinct_site_tariffs")
)
