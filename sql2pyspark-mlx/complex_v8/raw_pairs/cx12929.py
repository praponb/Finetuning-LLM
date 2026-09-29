from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_site_inst = Window.partitionBy("site_id", "quality").orderBy(F.col("site_installed_kw").desc(), "id").rowsBetween(-3, -1)
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_site_inst_2 = Window.partitionBy("site_id", "quality").orderBy(F.col("site_installed_kw").desc())
w_site_id_quality_by_site_inst_3 = Window.partitionBy("site_id", "quality").orderBy("site_installed_kw", "id")
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_site_id_quality_by_site_inst_4 = Window.partitionBy("site_id", "quality").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_kwh_prior_3 = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(-3, -1)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_site_installed_kw_pri = Window.orderBy("site_installed_kw", "id").rowsBetween(-6, -1)
w_quality_site_id_by_site_inst = Window.partitionBy("quality", "site_id").orderBy("site_installed_kw", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_by_first_known_site = Window.partitionBy("quality").orderBy(F.col("first_known_site_installed_kw_2").desc(), "id")
w_site_installed_kw_quartile = Window.partitionBy("site_installed_kw_quartile")
w_quality_by_first_known_site_2 = Window.partitionBy("quality").orderBy(F.col("first_known_site_installed_kw_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_distinct_quality = Window.partitionBy("quality").orderBy(F.col("distinct_quality_in_site_id").desc_nulls_first(), "id")
w_quality_by_distinct_quality_2 = Window.partitionBy("quality").orderBy(F.col("distinct_quality_in_site_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_quality_in_site_id = Window.partitionBy("distinct_quality_in_site_id")
w_n_quality_in_bad_ok_by_n_qua = Window.partitionBy("n_quality_in_bad_ok").orderBy("n_quality_in_est_bad", "distinct_quality_in_site_id", "site_id")
w_cnt_by_cnt_moving_2 = Window.partitionBy("cnt").orderBy("cnt", "distinct_quality_in_site_id", "site_id").rowsBetween(-2, Window.currentRow)

base_readings = (
    df_meter_readings
    .filter((F.col("site_id") <= 9) | (F.col("voltage") > 222.6))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

enriched_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.quality_slug",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

with_trends = (
    enriched_site
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "site_installed_kw",
        F.round(F.avg("voltage").over(w_site_id_quality_by_site_inst), 2).alias("voltage_avg_3_prior"),
        F.count(F.lit(1)).over(w_site_id_quality).alias("rows_in_site_id_quality"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.dense_rank().over(w_site_id_quality_by_site_inst_2).alias("site_installed_kw_dense_rank"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.percent_rank().over(w_site_id_quality_by_site_inst_3), 3).alias("site_installed_kw_percent_rank"),
        F.first("site_installed_kw", ignorenulls=True).over(w_site_id_by_kwh_running).alias("first_known_site_installed_kw"),
        F.lag("kwh", 2).over(w_site_id_by_kwh).alias("prev2_kwh"),
        F.first("site_installed_kw", ignorenulls=True).over(w_site_id_quality_by_site_inst_4).alias("first_known_site_installed_kw_2"),
        F.sum("site_installed_kw").over(w_site_id_by_kwh_prior_3).alias("site_installed_kw_sum_3_prior"),
        F.max("kwh").over(w_site_id_quality_by_site_inst_4).alias("max_kwh_so_far"),
        F.round(F.avg("site_installed_kw").over(w_all_by_site_installed_kw_pri), 2).alias("site_installed_kw_avg_6_prior"),
        F.last("voltage").over(w_quality_site_id_by_site_inst).alias("last_voltage"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_id_quality), 2).alias("site_installed_kw_vs_site_id_quality_avg"),
        F.ntile(4).over(w_site_id_quality_by_site_inst_3).alias("site_installed_kw_quartile"),
    )
)

leaders = (
    with_trends
    .filter(F.col("site_installed_kw_dense_rank") == 1)
    .drop("site_installed_kw_dense_rank")
)

windowed = (
    leaders
    .select(
        "*",
        (F.col("distinct_quality_in_site_id") - F.lag("distinct_quality_in_site_id").over(w_quality_by_first_known_site)).alias("distinct_quality_in_site_id_change"),
        F.round(F.col("rows_in_site_id_quality") - F.avg("rows_in_site_id_quality").over(w_site_installed_kw_quartile), 2).alias("rows_in_site_id_quality_vs_site_installed_kw_qua"),
        F.first("kwh_vs_site_id_avg", ignorenulls=True).over(w_quality_by_first_known_site_2).alias("first_known_kwh_vs_site_id_avg"),
        F.round(F.avg("distinct_quality_in_site_id").over(w_site_installed_kw_quartile), 2).alias("site_installed_kw_quartile_avg_distinct_quality_"),
        F.row_number().over(w_quality_by_distinct_quality).alias("seq_no"),
        F.nth_value("kwh_vs_site_id_avg", 2).over(w_quality_by_distinct_quality_2).alias("second_kwh_vs_site_id_avg"),
    )
)

grouped = (
    windowed
    .groupBy("distinct_quality_in_site_id", "site_id")
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.round(100 * F.count(F.when(F.col("quality") == "EST", 1)) / F.count("*"), 1).alias("pct_quality_est"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("n_quality_in_bad_ok"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("quality").isin("EST", "BAD"), 1)).alias("n_quality_in_est_bad"),
        F.round(F.sum("distinct_quality_in_site_id") / F.nullif(F.sum("site_installed_kw"), F.lit(0)), 3).alias("distinct_quality_in_site_id_to_site_installed_kw"),
        F.max("site_installed_kw").alias("max_site_installed_kw_check"),
    )
    .filter(F.col("max_site_installed_kw_check") > 141.6)
    .drop("max_site_installed_kw_check")
)

with_windows = (
    grouped
    .select(
        "distinct_quality_in_site_id",
        "site_id",
        "quality_list",
        "n_quality_in_est_bad",
        "distinct_quality_in_site_id_to_site_installed_kw",
        F.round(F.avg("pct_quality_est").over(w_distinct_quality_in_site_id), 2).alias("distinct_quality_in_site_id_avg_pct_quality_est"),
        F.ntile(3).over(w_n_quality_in_bad_ok_by_n_qua).alias("n_quality_in_est_bad_tercile"),
        F.sum("cnt").over(w_cnt_by_cnt_moving_2).alias("cnt_sum_3"),
    )
)

result = (
    with_windows
    .select(
        "distinct_quality_in_site_id",
        "site_id",
        "quality_list",
        "n_quality_in_est_bad",
        "distinct_quality_in_site_id_to_site_installed_kw",
        "distinct_quality_in_site_id_avg_pct_quality_est",
        "n_quality_in_est_bad_tercile",
        "cnt_sum_3",
        F.abs(F.col("cnt_sum_3") - 24).alias("cnt_sum_3_distance"),
        F.round(F.col("n_quality_in_est_bad") / F.nullif(F.col("cnt_sum_3"), F.lit(0)), 3).alias("n_quality_in_est_bad_per_cnt_sum_3"),
    )
    .orderBy(F.col("site_id").asc_nulls_last())
)
