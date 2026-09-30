from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_month_by_read_at_hou = Window.partitionBy(F.month("read_at")).orderBy("read_at_hour", "id")
w_all_by_voltage_prior_2 = Window.orderBy("voltage", "id").rowsBetween(-2, -1)
w_site_id_by_read_at_hour_ties = Window.partitionBy("site_id").orderBy("read_at_hour")
w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_running = Window.orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at_hour_full = Window.partitionBy("site_id").orderBy("read_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_voltage_moving_2 = Window.orderBy("voltage", "id").rowsBetween(-2, Window.currentRow)
w_site_id_read_at_hour_rank_by = Window.partitionBy("site_id", "read_at_hour_rank").orderBy(F.col("running_kwh").desc(), "id")
w_site_id_by_rows_in_site_id = Window.partitionBy("site_id").orderBy(F.col("rows_in_site_id").desc(), "id")
w_read_at_hour_month = Window.partitionBy(F.month("read_at_hour"))
w_read_at_hour_rank = Window.partitionBy("read_at_hour_rank")

prepared = (
    df_meter_readings
    .filter(F.col("site_id") >= 6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_trunc("hour", F.col("read_at")).alias("read_at_hour"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.lag("read_at").over(w_read_at_month_by_read_at_hou).alias("prev_read_at"),
        F.sum("kwh").over(w_all_by_voltage_prior_2).alias("kwh_sum_2_prior"),
        F.rank().over(w_site_id_by_read_at_hour_ties).alias("read_at_hour_rank"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.sum("kwh").over(w_all_by_voltage_running).alias("running_kwh"),
        F.round(F.cume_dist().over(w_read_at_month_by_read_at_hou), 3).alias("read_at_hour_cume_dist"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
        F.nth_value("kwh", 2).over(w_site_id_by_read_at_hour_full).alias("second_kwh"),
        F.last("read_at_hour", ignorenulls=True).over(w_all_by_voltage_running).alias("last_known_read_at_hour"),
        F.round(F.avg("kwh").over(w_all_by_voltage_moving_2), 2).alias("kwh_avg_3"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.lead("prev_read_at", 3).over(w_site_id_read_at_hour_rank_by).alias("next3_prev_read_at"),
        F.lag("read_at_hour_rank").over(w_site_id_by_rows_in_site_id).alias("prev_read_at_hour_rank"),
        F.round(F.avg("rows_in_site_id").over(w_read_at_hour_month), 2).alias("read_at_hour_month_avg_rows_in_site_id"),
        F.min("kwh_sum_2_prior").over(w_read_at_hour_rank).alias("read_at_hour_rank_min_kwh_sum_2_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "read_at_hour",
        "prev_read_at",
        "kwh_sum_2_prior",
        "rows_in_site_id",
        "voltage_share_of_site_id",
        "running_kwh",
        "voltage_vs_site_id_avg",
        "second_kwh",
        "last_known_read_at_hour",
        "kwh_avg_3",
        "next3_prev_read_at",
        "prev_read_at_hour_rank",
        "read_at_hour_month_avg_rows_in_site_id",
        "read_at_hour_rank_min_kwh_sum_2_prior",
        F.round((F.unix_timestamp(F.col("read_at_hour")) - F.unix_timestamp(F.col("last_known_read_at_hour"))) / 3600, 1).alias("hours_last_known_read_at_hour_to_read_at_hour"),
        F.round(F.col("kwh_sum_2_prior") / F.nullif(F.col("voltage_vs_site_id_avg"), F.lit(0)), 3).alias("kwh_sum_2_prior_per_voltage_vs_site_id_avg"),
    )
)
