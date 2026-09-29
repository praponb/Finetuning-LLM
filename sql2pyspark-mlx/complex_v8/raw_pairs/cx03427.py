from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_voltage = Window.orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_ties = Window.orderBy("voltage")
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_kwh_running = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_voltage_vs_site_i = Window.partitionBy("site_id").orderBy("voltage_vs_site_id_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_read_at_month_site_id_b = Window.partitionBy(F.date_trunc("month", "last_read_at"), "site_id").orderBy(F.col("running_voltage").desc(), "id")
w_last_read_at_month_site_id_b_2 = Window.partitionBy(F.date_trunc("month", "last_read_at"), "site_id").orderBy(F.col("running_voltage").desc())
w_last_read_at_month_site_id_b_3 = Window.partitionBy(F.date_trunc("month", "last_read_at"), "site_id").orderBy(F.col("running_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_read_at_month_site_id_b_4 = Window.partitionBy(F.date_trunc("month", "last_read_at"), "site_id").orderBy(F.col("running_voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_readings = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
    )
)

metrics = (
    recent_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.min("kwh").over(w_site_id).alias("site_id_min_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
        F.last("read_at").over(w_all_by_read_at_full).alias("last_read_at"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.first("kwh").over(w_site_id_by_voltage_full).alias("first_kwh"),
        F.sum("voltage").over(w_site_id_by_kwh_running).alias("running_voltage"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id), 2).alias("voltage_vs_site_id_avg"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.nth_value("voltage_rank", 2).over(w_site_id_by_voltage_vs_site_i).alias("second_voltage_rank"),
        (F.col("voltage") - F.lag("voltage").over(w_last_read_at_month_site_id_b)).alias("voltage_change"),
        F.nth_value("first_kwh", 2).over(w_site_id_by_voltage_vs_site_i).alias("second_first_kwh"),
        F.dense_rank().over(w_last_read_at_month_site_id_b_2).alias("running_voltage_dense_rank"),
        F.last("first_kwh", ignorenulls=True).over(w_last_read_at_month_site_id_b_3).alias("last_known_first_kwh"),
        F.count(F.lit(1)).over(w_last_read_at_month_site_id_b_3).alias("running_count"),
        F.last("kwh_vs_site_id_avg").over(w_last_read_at_month_site_id_b_4).alias("last_kwh_vs_site_id_avg"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
    )
)

result = (
    ranked
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "minutes_since_prev_read_at",
        "site_id_min_kwh",
        "kwh_vs_site_id_avg",
        "voltage_rank",
        "last_read_at",
        "voltage_share_of_site_id",
        "voltage_vs_site_id_avg",
        "second_first_kwh",
        "running_voltage_dense_rank",
        "last_known_first_kwh",
        "last_kwh_vs_site_id_avg",
        "rows_in_site_id",
        F.round(F.col("last_known_first_kwh") / F.nullif(F.col("first_kwh"), F.lit(0)), 3).alias("last_known_first_kwh_per_first_kwh"),
    )
    .orderBy(F.col("site_id_min_kwh").desc())
)
