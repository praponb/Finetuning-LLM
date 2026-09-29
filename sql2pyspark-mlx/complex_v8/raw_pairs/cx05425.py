from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_site_id_by_voltage_full = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_month_site_id_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("kwh", "id")
w_read_at_month_site_id_by_kwh_2 = Window.partitionBy(F.date_trunc("month", "read_at"), "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_voltage_running = Window.partitionBy("site_id").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_second_read_at_pr = Window.partitionBy("site_id").orderBy(F.col("second_read_at").asc_nulls_last(), "id").rowsBetween(-1, -1)
w_site_id_by_second_read_at_ru = Window.partitionBy("site_id").orderBy(F.col("second_read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_by_last_known_kwh_pr = Window.partitionBy("site_id").orderBy(F.col("last_known_kwh").desc(), "id").rowsBetween(-2, -1)
w_second_read_at_to_date = Window.partitionBy(F.to_date("second_read_at"))
w_site_id_by_last_known_kwh = Window.partitionBy("site_id").orderBy(F.col("last_known_kwh").desc(), "id")
w_site_id_by_last_known_kwh_fu = Window.partitionBy("site_id").orderBy(F.col("last_known_kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_by_second_read_at_fu = Window.partitionBy("site_id").orderBy(F.col("second_read_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_meter_readings
    .filter(F.col("voltage") > 218.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.substring(F.trim(F.col("quality")), 1, 2).alias("quality_prefix"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.lead("kwh", 1, 0).over(w_site_id_by_voltage).alias("next_kwh"),
        F.last("voltage").over(w_site_id_by_voltage_full).alias("last_voltage"),
        F.lag("read_at").over(w_read_at_month_site_id_by_kwh).alias("prev_read_at"),
        F.nth_value("read_at", 2).over(w_read_at_month_site_id_by_kwh_2).alias("second_read_at"),
        F.last("kwh", ignorenulls=True).over(w_site_id_by_voltage_running).alias("last_known_kwh"),
    )
)

labeled = (
    ranked
    .filter(F.col("read_at_label").isNotNull())
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("second_read_at")) - F.unix_timestamp(F.col("read_at"))) / 3600, 1).alias("hours_read_at_to_second_read_at"),
        F.lower(F.regexp_replace(F.trim(F.col("read_at_label")), " ", "_")).alias("read_at_label_slug"),
    )
)

with_trends = (
    labeled
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "read_at_label",
        "quality_prefix",
        "voltage_share_of_site_id",
        "site_id_avg_kwh",
        "next_kwh",
        "last_voltage",
        "prev_read_at",
        "last_known_kwh",
        "hours_read_at_to_second_read_at",
        F.round(F.avg("last_voltage").over(w_site_id_by_second_read_at_pr), 2).alias("last_voltage_avg_1_prior"),
        F.count(F.lit(1)).over(w_site_id_by_second_read_at_ru).alias("running_count"),
        F.sum("kwh").over(w_site_id_by_last_known_kwh_pr).alias("kwh_sum_2_prior"),
        F.size(F.collect_set("site_id").over(w_second_read_at_to_date)).alias("distinct_site_id_in_second_read_at_to_date"),
        F.lag("prev_read_at", 3).over(w_site_id_by_last_known_kwh).alias("prev3_prev_read_at"),
        F.max("last_voltage").over(w_second_read_at_to_date).alias("second_read_at_to_date_max_last_voltage"),
        F.last("second_read_at").over(w_site_id_by_last_known_kwh_fu).alias("last_second_read_at"),
        F.first("prev_read_at").over(w_site_id_by_second_read_at_fu).alias("first_prev_read_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "kwh",
        "read_at_label",
        "site_id_avg_kwh",
        "next_kwh",
        "last_voltage",
        "prev_read_at",
        "last_known_kwh",
        "hours_read_at_to_second_read_at",
        "last_voltage_avg_1_prior",
        "running_count",
        "distinct_site_id_in_second_read_at_to_date",
        "prev3_prev_read_at",
        (F.col("last_voltage_avg_1_prior") + F.col("last_known_kwh")).alias("last_voltage_avg_1_prior_plus_last_known_kwh"),
        F.abs(F.col("running_count") - 10).alias("running_count_distance"),
    )
)
