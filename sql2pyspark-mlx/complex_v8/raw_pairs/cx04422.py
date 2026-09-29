from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_moving_3 = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_site_id_read_at_month = Window.partitionBy("site_id", F.date_trunc("month", "read_at"))
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_by_kwh = Window.partitionBy("site_id").orderBy("kwh", "id")
w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first())
w_site_id_by_kwh_full = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh_prior_1 = Window.partitionBy("site_id").orderBy("kwh", "id").rowsBetween(-1, -1)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc_nulls_first(), "id")

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        (F.col("voltage") + F.col("kwh")).alias("voltage_plus_kwh"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.max("kwh").over(w_site_id_by_voltage_moving_3).alias("kwh_max_4"),
        F.count(F.lit(1)).over(w_site_id_read_at_month).alias("rows_in_site_id_read_at_month"),
        F.round(F.cume_dist().over(w_site_id_by_read_at), 3).alias("read_at_cume_dist"),
        F.round(F.percent_rank().over(w_site_id_by_kwh), 3).alias("kwh_percent_rank"),
        F.dense_rank().over(w_site_id_by_voltage_ties).alias("voltage_dense_rank"),
        F.last("kwh").over(w_site_id_by_kwh_full).alias("last_kwh"),
        F.round(F.avg("kwh").over(w_site_id_read_at_month), 2).alias("site_id_read_at_month_avg_kwh"),
        F.sum("kwh").over(w_site_id_by_kwh_prior_1).alias("kwh_sum_1_prior"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_kwh)).alias("voltage_change"),
        F.sum("voltage").over(w_site_id_by_voltage_running).alias("running_voltage"),
        F.lead("read_at").over(w_site_id_by_voltage).alias("next_read_at"),
        F.first("kwh").over(w_site_id_by_kwh_full).alias("first_kwh"),
        F.sum("voltage").over(w_site_id_by_voltage_moving_3).alias("voltage_sum_4"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
    )
)

result = (
    with_windows
    .filter(F.col("kwh_percent_rank") >= 0.1)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage_plus_kwh",
        "kwh_max_4",
        "rows_in_site_id_read_at_month",
        "read_at_cume_dist",
        "voltage_dense_rank",
        "site_id_read_at_month_avg_kwh",
        "voltage_change",
        "running_voltage",
        "first_kwh",
        "voltage_sum_4",
        F.abs(F.col("read_at_cume_dist") - 0.3).alias("read_at_cume_dist_distance"),
    )
    .orderBy(F.col("read_at").desc(), "id")
    .limit(10)
)
