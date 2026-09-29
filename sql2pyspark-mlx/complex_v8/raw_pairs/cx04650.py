from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_month = Window.partitionBy("quality", F.date_trunc("month", "read_at"))
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id")
w_quality_by_read_at = Window.partitionBy("quality").orderBy(F.col("read_at").desc(), "id")
w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_by_voltage_moving_1 = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_quality_site_id = Window.partitionBy("quality", "site_id")
w_site_id_quality_by_kwh_full = Window.partitionBy("site_id", "quality").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_kwh_ties = Window.partitionBy("site_id", "quality").orderBy("kwh")
w_quality_site_id_by_read_at_t = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc())
w_site_id_quality_by_kwh_prior = Window.partitionBy("site_id", "quality").orderBy("kwh", "id").rowsBetween(-3, -1)
w_quality_site_id_by_read_at_f = Window.partitionBy("quality", "site_id").orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_quality_read_at_month = Window.orderBy(F.col("quality_read_at_month_sum_voltage").desc())
w_all_by_quality_read_at_month_2 = Window.orderBy(F.col("quality_read_at_month_sum_voltage").desc(), "id")
w_read_at_dense_rank_distinct = Window.partitionBy("read_at_dense_rank", "distinct_site_id_in_quality_read_at_month").orderBy("kwh_max_4")
w_site_id_distinct_site_id_in = Window.partitionBy("site_id", "distinct_site_id_in_quality_read_at_month")

scoped = (
    df_meter_readings
    .filter(F.col("voltage") < 239.6)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

windowed = (
    scoped
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_quality_read_at_month)).alias("distinct_site_id_in_quality_read_at_month"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_voltage)).alias("voltage_change"),
        F.round(F.avg("voltage").over(w_quality_read_at_month), 2).alias("quality_read_at_month_avg_voltage"),
        F.dense_rank().over(w_quality_by_read_at).alias("read_at_dense_rank"),
        F.max("kwh").over(w_site_id_by_voltage_moving_3).alias("kwh_max_4"),
        F.max("kwh").over(w_site_id_by_voltage_moving_1).alias("kwh_max_2"),
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_quality_site_id), F.lit(0)), 4).alias("kwh_share_of_quality_site_id"),
        F.nth_value("voltage", 2).over(w_site_id_quality_by_kwh_full).alias("second_voltage"),
        F.count(F.lit(1)).over(w_quality_site_id).alias("rows_in_quality_site_id"),
        F.sum("voltage").over(w_quality_read_at_month).alias("quality_read_at_month_sum_voltage"),
        F.round(F.cume_dist().over(w_site_id_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.percent_rank().over(w_quality_site_id_by_read_at_t), 3).alias("read_at_percent_rank"),
        F.sum("kwh").over(w_site_id_quality_by_kwh_prior).alias("kwh_sum_3_prior"),
        F.nth_value("voltage", 2).over(w_quality_site_id_by_read_at_f).alias("second_voltage_2"),
        F.nth_value("kwh", 2).over(w_site_id_by_voltage_full).alias("second_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "distinct_site_id_in_quality_read_at_month",
        "quality_read_at_month_avg_voltage",
        "read_at_dense_rank",
        "kwh_max_4",
        "kwh_max_2",
        "kwh_share_of_quality_site_id",
        "rows_in_quality_site_id",
        "quality_read_at_month_sum_voltage",
        "kwh_cume_dist",
        "second_voltage_2",
        "second_kwh",
        "voltage_vs_quality_site_id_avg",
        F.dense_rank().over(w_all_by_quality_read_at_month).alias("quality_read_at_month_sum_voltage_dense_rank"),
        F.row_number().over(w_all_by_quality_read_at_month_2).alias("rn"),
        F.round(F.cume_dist().over(w_read_at_dense_rank_distinct), 3).alias("kwh_max_4_cume_dist"),
        F.count(F.lit(1)).over(w_site_id_distinct_site_id_in).alias("rows_in_site_id_distinct_site_id_in_quality_read"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "voltage",
        "distinct_site_id_in_quality_read_at_month",
        "quality_read_at_month_avg_voltage",
        "read_at_dense_rank",
        "kwh_max_4",
        "kwh_share_of_quality_site_id",
        "rows_in_quality_site_id",
        "kwh_cume_dist",
        "second_voltage_2",
        "second_kwh",
        "voltage_vs_quality_site_id_avg",
        "quality_read_at_month_sum_voltage_dense_rank",
        "rn",
        "rows_in_site_id_distinct_site_id_in_quality_read",
    )
    .orderBy(F.col("read_at_dense_rank").asc_nulls_last())
)
