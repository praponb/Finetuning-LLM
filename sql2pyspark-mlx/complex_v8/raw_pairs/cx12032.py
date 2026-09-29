from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_kwh_ties = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last())
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy("voltage")
w_kwh_decile_by_kwh_ties = Window.partitionBy("kwh_decile").orderBy(F.col("kwh").desc())
w_site_id_by_quality_sum_kwh = Window.partitionBy("site_id").orderBy("quality_sum_kwh", "id")
w_quality_by_rows_in_quality = Window.partitionBy("quality").orderBy(F.col("rows_in_quality").asc_nulls_last(), "id")
w_site_id_by_quality_sum_kwh_r = Window.partitionBy("site_id").orderBy("quality_sum_kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_decile = Window.partitionBy("kwh_decile")
w_all_by_kwh_cume_dist_to_kwh = Window.orderBy(F.col("kwh_cume_dist_to_kwh_ratio").desc(), "rows_in_quality_seq", "site_id")
w_n_quality_in_bad_est = Window.partitionBy("n_quality_in_bad_est")

filtered = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality), F.lit(0)), 4).alias("voltage_share_of_quality"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality), 2).alias("voltage_vs_quality_avg"),
        F.min("kwh").over(w_quality).alias("quality_min_kwh"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
        F.sum("voltage").over(w_quality).alias("quality_sum_voltage"),
        F.first("kwh", ignorenulls=True).over(w_site_id_by_voltage_running).alias("first_known_kwh"),
        F.round(F.cume_dist().over(w_quality_site_id_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.sum("kwh").over(w_quality).alias("quality_sum_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.ntile(10).over(w_quality_site_id_by_kwh).alias("kwh_decile"),
        F.round(F.percent_rank().over(w_site_id_by_voltage_ties), 3).alias("voltage_percent_rank"),
    )
)

labeled = (
    with_trends
    .filter(F.col("kwh_vs_quality_avg") > -48.0)
    .select(
        "*",
        F.concat_ws("-", F.col("quality"), F.col("read_at_label")).alias("quality_read_at_label_label"),
        F.round(F.col("rows_in_quality") / F.nullif(F.col("quality_min_kwh"), F.lit(0)), 3).alias("rows_in_quality_per_quality_min_kwh"),
    )
)

with_trends_2 = (
    labeled
    .select(
        "*",
        F.rank().over(w_kwh_decile_by_kwh_ties).alias("kwh_rank"),
        F.ntile(4).over(w_site_id_by_quality_sum_kwh).alias("quality_sum_kwh_quartile"),
        F.row_number().over(w_quality_by_rows_in_quality).alias("rows_in_quality_seq"),
        F.last("voltage_share_of_quality", ignorenulls=True).over(w_site_id_by_quality_sum_kwh_r).alias("last_known_voltage_share_of_quality"),
        F.lag("kwh_cume_dist").over(w_site_id_by_quality_sum_kwh).alias("prev_kwh_cume_dist"),
        F.min("kwh_decile").over(w_kwh_decile).alias("kwh_decile_min_kwh_decile"),
        F.round(F.percent_rank().over(w_kwh_decile_by_kwh_ties), 3).alias("kwh_percent_rank"),
    )
)

grouped = (
    with_trends_2
    .filter(F.col("kwh_decile_min_kwh_decile") <= 55)
    .groupBy("rows_in_quality_seq", "site_id")
    .agg(
        F.sum("rows_in_quality_per_quality_min_kwh").alias("total_rows_in_quality_per_quality_min_kwh"),
        F.round(F.sum("kwh_cume_dist") / F.nullif(F.sum("kwh"), F.lit(0)), 3).alias("kwh_cume_dist_to_kwh_ratio"),
        F.sum(F.when(F.col("quality").isin("BAD", "EST"), 1).otherwise(0)).alias("n_quality_in_bad_est"),
        F.max("kwh").alias("max_kwh"),
    )
)

with_trends_3 = (
    grouped
    .select(
        "rows_in_quality_seq",
        "site_id",
        "total_rows_in_quality_per_quality_min_kwh",
        "n_quality_in_bad_est",
        "max_kwh",
        (F.col("max_kwh") - F.lag("max_kwh").over(w_all_by_kwh_cume_dist_to_kwh)).alias("max_kwh_change"),
        F.round(F.col("max_kwh") / F.nullif(F.sum("max_kwh").over(w_n_quality_in_bad_est), F.lit(0)), 4).alias("max_kwh_share_of_n_quality_in_bad_est"),
        F.max("kwh_cume_dist_to_kwh_ratio").over(w_n_quality_in_bad_est).alias("n_quality_in_bad_est_max_kwh_cume_dist_to_kwh_ra"),
    )
)

result = (
    with_trends_3
    .filter(F.col("total_rows_in_quality_per_quality_min_kwh") > 12.0)
    .select(
        "rows_in_quality_seq",
        "site_id",
        "total_rows_in_quality_per_quality_min_kwh",
        "n_quality_in_bad_est",
        "n_quality_in_bad_est_max_kwh_cume_dist_to_kwh_ra",
        F.round(F.col("max_kwh") * 0.9, 2).alias("max_kwh_adj"),
    )
    .orderBy(
        F.col("n_quality_in_bad_est").desc(),
        "total_rows_in_quality_per_quality_min_kwh",
    )
)
