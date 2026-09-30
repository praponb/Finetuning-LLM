from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy("kwh", "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_site_id_quality_by_kwh_full = Window.partitionBy("site_id", "quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_prior_1 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id").rowsBetween(-1, -1)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_voltage_rank_quality_by_kwh = Window.partitionBy("voltage_rank", "quality").orderBy(F.col("kwh").desc(), "id")
w_read_at_month_second_read_at = Window.partitionBy(F.date_trunc("month", "read_at"), F.date_trunc("month", "second_read_at"))
w_voltage_rank_by_kwh_sum_1_pr = Window.partitionBy("voltage_rank").orderBy(F.col("kwh_sum_1_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_voltage_rank_by_kwh_sum_1_pr_2 = Window.partitionBy("voltage_rank").orderBy(F.col("kwh_sum_1_prior").asc_nulls_last())
w_voltage_rank_by_kwh_sum_1_pr_3 = Window.partitionBy("voltage_rank").orderBy(F.col("kwh_sum_1_prior").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_sum_1_prior_full = Window.orderBy("kwh_sum_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_kwh_sum_1_prior_ties = Window.orderBy("kwh_sum_1_prior")

base_readings = (
    df_meter_readings
    .filter((F.col("kwh").between(14.5, 110.5)) & (F.hour(F.col("read_at")).between(8, 14)))
)

with_trends = (
    base_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.row_number().over(w_site_id_quality_by_kwh).alias("seq_no"),
        F.rank().over(w_site_id_by_voltage_ties).alias("voltage_rank"),
        F.round(F.percent_rank().over(w_all_by_voltage), 3).alias("voltage_percent_rank"),
        F.nth_value("read_at", 2).over(w_site_id_quality_by_kwh_full).alias("second_read_at"),
        F.sum("kwh").over(w_site_id_by_voltage_prior_1).alias("kwh_sum_1_prior"),
        F.round(F.avg("voltage").over(w_quality_site_id), 2).alias("quality_site_id_avg_voltage"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

windowed = (
    with_trends
    .select(
        "*",
        F.ntile(5).over(w_voltage_rank_quality_by_kwh).alias("kwh_quintile"),
        F.sum("voltage_percent_rank").over(w_read_at_month_second_read_at).alias("read_at_month_second_read_at_month_sum_voltage_p"),
        F.max("kwh").over(w_voltage_rank_by_kwh_sum_1_pr).alias("max_kwh_so_far"),
        F.round(F.cume_dist().over(w_voltage_rank_by_kwh_sum_1_pr_2), 3).alias("kwh_sum_1_prior_cume_dist"),
        F.nth_value("voltage_percent_rank", 2).over(w_voltage_rank_by_kwh_sum_1_pr_3).alias("second_voltage_percent_rank"),
        F.first("read_at").over(w_all_by_kwh_sum_1_prior_full).alias("first_read_at"),
        F.round(F.cume_dist().over(w_all_by_kwh_sum_1_prior_ties), 3).alias("kwh_sum_1_prior_cume_dist_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "seq_no",
        "second_read_at",
        "kwh_sum_1_prior",
        "quality_site_id_avg_voltage",
        "rows_in_quality_site_id",
        "minutes_since_prev_read_at",
        "kwh_quintile",
        "read_at_month_second_read_at_month_sum_voltage_p",
        "max_kwh_so_far",
        "kwh_sum_1_prior_cume_dist",
        "second_voltage_percent_rank",
        "first_read_at",
        F.greatest(F.col("max_kwh_so_far"), F.col("kwh")).alias("greatest_max_kwh_so_far_kwh"),
    )
    .orderBy(F.col("kwh_sum_1_prior").asc_nulls_last())
)
