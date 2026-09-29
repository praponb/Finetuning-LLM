from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_full = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_site_id_by_kwh_runni = Window.partitionBy("quality", "site_id").orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_by_read_at_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_last_known_read_at_m = Window.partitionBy("quality", F.date_trunc("month", "last_known_read_at"))
w_site_id = Window.partitionBy("site_id")
w_n_quality_in_ok_est_voltage = Window.partitionBy("n_quality_in_ok_est", "voltage_tier").orderBy("first_last_known_read_at", "distinct_site_id_in_quality_last_known_read_at_m", "read_at_ym", "voltage_tier").rowsBetween(-6, Window.currentRow)
w_n_quality_in_ok_est_voltage_2 = Window.partitionBy("n_quality_in_ok_est", "voltage_tier").orderBy("first_last_known_read_at", "distinct_site_id_in_quality_last_known_read_at_m", "read_at_ym", "voltage_tier")
w_n_quality_in_ok_est_voltage_3 = Window.partitionBy("n_quality_in_ok_est", "voltage_tier").orderBy("first_last_known_read_at")

base = (
    df_meter_readings
    .filter((F.month(F.col("read_at")).isin(1, 3)) | (F.col("quality") == "OK"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

ranked = (
    base
    .select(
        "*",
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.first("kwh", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_kwh"),
        F.nth_value("kwh", 2).over(w_all_by_read_at_full).alias("second_kwh"),
        F.last("read_at", ignorenulls=True).over(w_quality_site_id_by_kwh_runni).alias("last_known_read_at"),
        F.last("kwh", ignorenulls=True).over(w_quality_site_id_by_kwh_runni).alias("last_known_kwh"),
        F.min("voltage").over(w_quality_by_read_at_running).alias("min_voltage_so_far"),
        F.max("voltage").over(w_quality).alias("quality_max_voltage"),
        F.size(F.collect_set("site_id").over(w_quality)).alias("distinct_site_id_in_quality"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_quality_last_known_read_at_m)).alias("distinct_site_id_in_quality_last_known_read_at_m"),
        F.round(F.col("quality_max_voltage") / F.nullif(F.sum("quality_max_voltage").over(w_site_id), F.lit(0)), 4).alias("quality_max_voltage_share_of_site_id"),
        F.max("second_kwh").over(w_quality_last_known_read_at_m).alias("quality_last_known_read_at_month_max_second_kwh"),
    )
)

per_group = (
    windowed
    .filter(F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .groupBy(
        "distinct_site_id_in_quality_last_known_read_at_m",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
        F.when(F.col("voltage") >= 233.2, "A").when(F.col("voltage") >= 218.4, "B").otherwise("C").alias("voltage_tier"),
    )
    .agg(
        F.min("last_known_read_at").alias("first_last_known_read_at"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.sum(F.when(F.col("quality").isin("OK", "EST"), 1).otherwise(0)).alias("n_quality_in_ok_est"),
        F.count("*").alias("row_count"),
    )
)

with_trends = (
    per_group
    .select(
        "distinct_site_id_in_quality_last_known_read_at_m",
        "read_at_ym",
        "voltage_tier",
        "row_count",
        F.round(F.avg("row_count").over(w_n_quality_in_ok_est_voltage), 2).alias("row_count_avg_7"),
        F.lag("first_last_known_read_at").over(w_n_quality_in_ok_est_voltage_2).alias("prev_first_last_known_read_at"),
        F.rank().over(w_n_quality_in_ok_est_voltage_2).alias("first_last_known_read_at_rank"),
        F.dense_rank().over(w_n_quality_in_ok_est_voltage_3).alias("first_last_known_read_at_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("first_last_known_read_at_rank") > 2)
    .select(
        "distinct_site_id_in_quality_last_known_read_at_m",
        "read_at_ym",
        "voltage_tier",
        "row_count",
        "row_count_avg_7",
        "prev_first_last_known_read_at",
        "first_last_known_read_at_dense_rank",
        F.lower(F.trim(F.col("read_at_ym"))).alias("read_at_ym_clean"),
        F.greatest(F.col("row_count_avg_7"), F.col("row_count")).alias("greatest_row_count_avg_7_row_count"),
    )
    .orderBy("voltage_tier", "first_last_known_read_at_dense_rank")
)
