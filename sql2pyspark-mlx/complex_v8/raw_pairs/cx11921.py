from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_len_by_read_at = Window.partitionBy("quality_len").orderBy("read_at", "id")
w_quality_len_by_read_at_full = Window.partitionBy("quality_len").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality_len = Window.partitionBy("quality_len")
w_all_by_quality_len_moving_6 = Window.orderBy("quality_len", "id").rowsBetween(-6, Window.currentRow)
w_site_id_prev_quality_len = Window.partitionBy("site_id", "prev_quality_len")
w_site_id_prev_quality_len_by = Window.partitionBy("site_id", "prev_quality_len").orderBy(F.col("rows_in_quality_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_prev_quality_len_by_2 = Window.partitionBy("site_id", "prev_quality_len").orderBy(F.col("rows_in_quality_len").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_first_known_quality = Window.partitionBy("site_id", "first_known_quality_len")
w_all_by_second_read_at = Window.orderBy("second_read_at", "id")
w_all_by_second_read_at_ties = Window.orderBy("second_read_at")
w_quality_len_prev_quality_len = Window.partitionBy("quality_len", "prev_quality_len").orderBy("quality_len", "id")

base = (
    df_meter_readings
    .filter(F.hour(F.col("read_at")).between(6, 9))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.lag("quality_len").over(w_quality_len_by_read_at).alias("prev_quality_len"),
        F.nth_value("read_at", 2).over(w_quality_len_by_read_at_full).alias("second_read_at"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_len), 2).alias("voltage_vs_quality_len_avg"),
        F.sum("quality_len").over(w_all_by_quality_len_moving_6).alias("quality_len_sum_7"),
        F.sum("quality_len").over(w_quality_len).alias("quality_len_sum_quality_len"),
        F.count(F.lit(1)).over(w_quality_len).alias("rows_in_quality_len"),
    )
)

qualified = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") < 232.4), "left_anti")
    .filter(F.col("kwh").between(42.1, 81.3))
)

with_windows_2 = (
    qualified
    .select(
        "*",
        F.round(F.col("prev_quality_len") - F.avg("prev_quality_len").over(w_site_id_prev_quality_len), 2).alias("prev_quality_len_vs_site_id_prev_quality_len_avg"),
        F.round(F.avg("prev_quality_len").over(w_site_id_prev_quality_len), 2).alias("site_id_prev_quality_len_avg_prev_quality_len"),
        F.first("quality_len", ignorenulls=True).over(w_site_id_prev_quality_len_by).alias("first_known_quality_len"),
        F.first("quality_len").over(w_site_id_prev_quality_len_by_2).alias("first_quality_len"),
    )
)

with_windows_3 = (
    with_windows_2
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id_first_known_quality).alias("rows_in_site_id_first_known_quality_len"),
        F.lead("quality_len_sum_quality_len").over(w_all_by_second_read_at).alias("next_quality_len_sum_quality_len"),
        F.round(F.percent_rank().over(w_all_by_second_read_at_ties), 3).alias("second_read_at_percent_rank"),
        F.round(F.col("quality_len_sum_7") / F.nullif(F.sum("quality_len_sum_7").over(w_site_id_first_known_quality), F.lit(0)), 4).alias("quality_len_sum_7_share_of_site_id_first_known_q"),
        (F.col("first_known_quality_len") - F.lag("first_known_quality_len").over(w_quality_len_prev_quality_len)).alias("first_known_quality_len_change"),
        F.dense_rank().over(w_quality_len_prev_quality_len).alias("quality_len_dense_rank"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.min("read_at").alias("first_reading_read_at"))
)

profiled = (
    with_windows_3.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at")
)

result = (
    profiled
    .filter(F.col("prev_quality_len_vs_site_id_prev_quality_len_avg") < 4.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_clean",
        "quality_len",
        "prev_quality_len",
        "voltage_vs_quality_len_avg",
        "quality_len_sum_7",
        "quality_len_sum_quality_len",
        "rows_in_quality_len",
        "site_id_prev_quality_len_avg_prev_quality_len",
        "rows_in_site_id_first_known_quality_len",
        "next_quality_len_sum_quality_len",
        "quality_len_dense_rank",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_reading_read_at")).alias("days_since_first_reading_read_at"),
    )
    .orderBy(
        "site_id_prev_quality_len_avg_prev_quality_len",
        F.col("voltage_vs_quality_len_avg").desc(),
    )
)
