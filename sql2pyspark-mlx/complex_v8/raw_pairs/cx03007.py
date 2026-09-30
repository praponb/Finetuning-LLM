from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_all_by_read_at_ties = Window.orderBy("read_at")
w_quality_len_site_id = Window.partitionBy("quality_len", "site_id")
w_quality_len_read_at_month_by = Window.partitionBy("quality_len", F.date_trunc("month", "read_at")).orderBy(F.col("voltage").asc_nulls_last(), "id")
w_all_by_read_at_running = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_quality_len_site_id_by_volta = Window.partitionBy("quality_len", "site_id").orderBy(F.col("voltage_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_len_read_at_month_by_2 = Window.partitionBy("quality_len", F.date_trunc("month", "read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_read_at_full = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_len_second_read_at_m = Window.partitionBy("quality_len", F.month("second_read_at")).orderBy("kwh", "id")
w_quality_len_by_quality_len_s = Window.partitionBy("quality_len").orderBy(F.col("quality_len_site_id_min_voltage_distance").asc_nulls_last(), "id")
w_quality_len_second_read_at_m_2 = Window.partitionBy("quality_len", F.month("second_read_at")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_second_read_at_prior = Window.orderBy(F.col("second_read_at").desc(), "id").rowsBetween(-1, -1)

base_readings = (
    df_meter_readings
    .filter((F.col("read_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("site_id") >= 2))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("voltage") - 232.7).alias("voltage_distance"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

windowed = (
    base_readings
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_read_at_to_date), F.lit(0)), 4).alias("voltage_share_of_read_at_to_date"),
        F.round(F.col("voltage_distance") / F.nullif(F.sum("voltage_distance").over(w_read_at_to_date), F.lit(0)), 4).alias("voltage_distance_share_of_read_at_to_date"),
        F.dense_rank().over(w_all_by_read_at_ties).alias("read_at_dense_rank"),
        F.min("voltage_distance").over(w_quality_len_site_id).alias("quality_len_site_id_min_voltage_distance"),
        F.round(F.percent_rank().over(w_quality_len_read_at_month_by), 3).alias("voltage_percent_rank"),
        F.first("quality_len", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_quality_len"),
        F.lead("read_at", 2).over(w_quality_len_read_at_month_by).alias("next2_read_at"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_quality_len_site_id), F.lit(0)), 4).alias("voltage_share_of_quality_len_site_id"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.nth_value("voltage_distance", 2).over(w_quality_len_site_id_by_volta).alias("second_voltage_distance"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_to_date), 2).alias("kwh_vs_read_at_to_date_avg"),
        (F.col("kwh") - F.lag("kwh").over(w_quality_len_read_at_month_by_2)).alias("kwh_change"),
        F.sum("quality_len").over(w_quality_len_site_id).alias("quality_len_site_id_sum_quality_len"),
        F.nth_value("read_at", 2).over(w_all_by_read_at_full).alias("second_read_at"),
        F.rank().over(w_all_by_read_at_ties).alias("read_at_rank"),
    )
)

first_rows = (
    windowed
    .filter((F.col("read_at_rank") <= 5) & (F.col("voltage_percent_rank").between(0.1, 0.7)))
)

windowed_2 = (
    first_rows
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_len",
        "voltage_share_of_read_at_to_date",
        "read_at_dense_rank",
        "quality_len_site_id_min_voltage_distance",
        "voltage_percent_rank",
        "first_known_quality_len",
        "next2_read_at",
        "minutes_since_prev_read_at",
        "kwh_vs_read_at_to_date_avg",
        (F.col("kwh") - F.lag("kwh").over(w_quality_len_second_read_at_m)).alias("kwh_change_2"),
        F.lag("voltage", 3).over(w_quality_len_by_quality_len_s).alias("prev3_voltage"),
        F.nth_value("kwh_change", 2).over(w_quality_len_second_read_at_m_2).alias("second_kwh_change"),
        F.max("voltage_percent_rank").over(w_all_by_second_read_at_prior).alias("voltage_percent_rank_max_1_prior"),
        F.lag("kwh_vs_read_at_to_date_avg", 1, 0).over(w_quality_len_by_quality_len_s).alias("prev_kwh_vs_read_at_to_date_avg"),
    )
)

result = (
    windowed_2
    .filter(F.col("minutes_since_prev_read_at").between(750.0, 4750.0))
    .select(
        "id",
        "site_id",
        "quality_len",
        "voltage_share_of_read_at_to_date",
        "read_at_dense_rank",
        "voltage_percent_rank",
        "first_known_quality_len",
        "next2_read_at",
        "minutes_since_prev_read_at",
        "kwh_change_2",
        "prev3_voltage",
        "prev_kwh_vs_read_at_to_date_avg",
        F.when(F.col("second_kwh_change") >= 60.0, "HIGH").when(F.col("second_kwh_change") >= -48.0, "MEDIUM").otherwise("LOW").alias("second_kwh_change_tier"),
    )
    .orderBy(F.col("read_at_dense_rank").desc(), "first_known_quality_len")
)
