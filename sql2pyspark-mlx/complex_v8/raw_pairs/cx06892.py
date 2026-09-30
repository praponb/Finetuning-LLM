from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id_by_voltage = Window.partitionBy("quality", "site_id").orderBy("voltage", "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_quality = Window.partitionBy("quality")
w_all_by_kwh = Window.orderBy("kwh", "id")
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_site_id_by_voltage_p = Window.partitionBy("quality", "site_id").orderBy("voltage", "id").rowsBetween(-1, -1)
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_seq_no_by_kwh_per_voltage_sh = Window.partitionBy("seq_no").orderBy("kwh_per_voltage_share_of_quality", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_kwh_rank_kwh_rank_2_by_volta = Window.partitionBy("kwh_rank", "kwh_rank_2").orderBy(F.col("voltage").desc(), "id")
w_prev_read_at_month = Window.partitionBy(F.date_trunc("month", "prev_read_at"))
w_all_by_seq_no_prior_2 = Window.orderBy("seq_no", "id").rowsBetween(-2, -1)
w_all_by_seq_no_ties = Window.orderBy("seq_no")
w_seq_no_by_kwh_per_voltage_sh_2 = Window.partitionBy("seq_no").orderBy("kwh_per_voltage_share_of_quality", "id")

prepared = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-18 10:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.row_number().over(w_quality_site_id_by_voltage).alias("seq_no"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.round(F.col("kwh_per_voltage") / F.nullif(F.sum("kwh_per_voltage").over(w_quality), F.lit(0)), 4).alias("kwh_per_voltage_share_of_quality"),
        F.ntile(5).over(w_all_by_kwh).alias("kwh_quintile"),
        F.min("voltage").over(w_quality).alias("quality_min_voltage"),
        F.lag("read_at").over(w_all_by_kwh).alias("prev_read_at"),
        F.count(F.lit(1)).over(w_all_by_kwh_running).alias("running_count"),
        F.first("read_at", ignorenulls=True).over(w_all_by_kwh_running).alias("first_known_read_at"),
        F.round(F.avg("voltage").over(w_quality_site_id_by_voltage_p), 2).alias("voltage_avg_1_prior"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.nth_value("kwh_per_voltage", 2).over(w_all_by_kwh_full).alias("second_kwh_per_voltage"),
        F.rank().over(w_all_by_kwh).alias("kwh_rank_2"),
        F.count(F.lit(1)).over(w_quality).alias("rows_in_quality"),
    )
)

tagged = (
    ranked
    .filter(F.col("kwh_per_voltage") > 4.0)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality_clean",
        "kwh_per_voltage",
        "seq_no",
        "kwh_dense_rank",
        "kwh_per_voltage_share_of_quality",
        "kwh_quintile",
        "quality_min_voltage",
        "prev_read_at",
        "running_count",
        "first_known_read_at",
        "voltage_avg_1_prior",
        "kwh_rank",
        "second_kwh_per_voltage",
        "kwh_rank_2",
        "rows_in_quality",
        F.round(F.col("kwh") / F.nullif(F.col("running_count"), F.lit(0)), 3).alias("kwh_per_running_count"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.min("kwh_quintile").over(w_seq_no_by_kwh_per_voltage_sh).alias("min_kwh_quintile_so_far"),
        F.lead("prev_read_at", 3).over(w_kwh_rank_kwh_rank_2_by_volta).alias("next3_prev_read_at"),
        F.round(F.col("kwh_rank") - F.avg("kwh_rank").over(w_prev_read_at_month), 2).alias("kwh_rank_vs_prev_read_at_month_avg"),
        F.round(F.col("quality_min_voltage") - F.avg("quality_min_voltage").over(w_prev_read_at_month), 2).alias("quality_min_voltage_vs_prev_read_at_month_avg"),
        F.count(F.lit(1)).over(w_prev_read_at_month).alias("rows_in_prev_read_at_month"),
        F.round(F.avg("kwh_rank_2").over(w_all_by_seq_no_prior_2), 2).alias("kwh_rank_2_avg_2_prior"),
        F.round(F.cume_dist().over(w_all_by_seq_no_ties), 3).alias("seq_no_cume_dist"),
        F.lag("kwh_rank_2").over(w_seq_no_by_kwh_per_voltage_sh_2).alias("prev_kwh_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "read_at",
        "voltage",
        "kwh_per_voltage",
        "seq_no",
        "kwh_per_voltage_share_of_quality",
        "kwh_quintile",
        "quality_min_voltage",
        "running_count",
        "first_known_read_at",
        "voltage_avg_1_prior",
        "kwh_rank",
        "second_kwh_per_voltage",
        "kwh_rank_2",
        "rows_in_quality",
        "min_kwh_quintile_so_far",
        "next3_prev_read_at",
        "kwh_rank_vs_prev_read_at_month_avg",
        "quality_min_voltage_vs_prev_read_at_month_avg",
        "rows_in_prev_read_at_month",
        "kwh_rank_2_avg_2_prior",
        F.substring(F.trim(F.col("quality_clean")), 1, 3).alias("quality_clean_prefix"),
        (F.floor(F.col("quality_min_voltage") / 500) * 500).alias("quality_min_voltage_bucket"),
    )
    .orderBy("kwh_per_voltage", "id")
    .limit(5)
)
