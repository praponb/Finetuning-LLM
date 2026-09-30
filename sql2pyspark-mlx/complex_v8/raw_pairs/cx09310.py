from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at = Window.orderBy("read_at", "id")
w_read_at_to_date_quality = Window.partitionBy(F.to_date("read_at"), "quality")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_prior_1 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id").rowsBetween(-1, -1)
w_all_by_read_at_2 = Window.orderBy(F.col("read_at").desc_nulls_first(), "id")
w_all_by_read_at_running_2 = Window.orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc_nulls_first())
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_seq = Window.partitionBy("read_at_seq")
w_distinct_site_id_in_read_at = Window.partitionBy("distinct_site_id_in_read_at_to_date_quality", "read_at_dense_rank").orderBy(F.col("kwh_vs_read_at_to_date_quality_avg").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_voltage_avg_1_prior_r = Window.orderBy("voltage_avg_1_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_known_read_at_to_date = Window.partitionBy(F.to_date("first_known_read_at"))

valid_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

with_windows = (
    valid_readings
    .select(
        "*",
        F.row_number().over(w_all_by_read_at).alias("read_at_seq"),
        F.size(F.collect_set("site_id").over(w_read_at_to_date_quality)).alias("distinct_site_id_in_read_at_to_date_quality"),
        F.first("read_at", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_to_date_quality), 2).alias("kwh_vs_read_at_to_date_quality_avg"),
        F.round(F.avg("voltage").over(w_all_by_read_at_prior_1), 2).alias("voltage_avg_1_prior"),
        F.max("read_at_hour").over(w_all_by_read_at_running).alias("max_read_at_hour_so_far"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_read_at_to_date_quality), 2).alias("voltage_vs_read_at_to_date_quality_avg"),
        F.dense_rank().over(w_all_by_read_at_2).alias("read_at_dense_rank"),
        F.round(F.avg("kwh").over(w_read_at_to_date_quality), 2).alias("read_at_to_date_quality_avg_kwh"),
        F.sum("read_at_hour").over(w_read_at_to_date_quality).alias("read_at_to_date_quality_sum_read_at_hour"),
        F.first("kwh", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_kwh"),
        F.count(F.lit(1)).over(w_all_by_read_at_running_2).alias("running_count"),
        F.round(F.percent_rank().over(w_all_by_read_at_ties), 3).alias("read_at_percent_rank"),
        F.sum("voltage").over(w_read_at_to_date_quality).alias("read_at_to_date_quality_sum_voltage"),
        F.last("read_at_hour").over(w_all_by_kwh_full).alias("last_read_at_hour"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.col("kwh") / F.nullif(F.sum("kwh").over(w_read_at_seq), F.lit(0)), 4).alias("kwh_share_of_read_at_seq"),
        F.round(F.col("max_read_at_hour_so_far") / F.nullif(F.sum("max_read_at_hour_so_far").over(w_read_at_seq), F.lit(0)), 4).alias("max_read_at_hour_so_far_share_of_read_at_seq"),
        F.round(F.avg("read_at_seq").over(w_distinct_site_id_in_read_at), 2).alias("read_at_seq_avg_2"),
        F.min("last_read_at_hour").over(w_all_by_voltage_avg_1_prior_r).alias("min_last_read_at_hour_so_far"),
        F.max("read_at_percent_rank").over(w_first_known_read_at_to_date).alias("first_known_read_at_to_date_max_read_at_percent_"),
        F.count(F.lit(1)).over(w_first_known_read_at_to_date).alias("rows_in_first_known_read_at_to_date"),
        F.min("voltage").over(w_read_at_seq).alias("read_at_seq_min_voltage"),
    )
)

result = (
    windowed
    .filter(F.col("rows_in_first_known_read_at_to_date") >= 4)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality_ref",
        "read_at_hour",
        "read_at_seq",
        "distinct_site_id_in_read_at_to_date_quality",
        "kwh_vs_read_at_to_date_quality_avg",
        "max_read_at_hour_so_far",
        "read_at_dense_rank",
        "read_at_to_date_quality_avg_kwh",
        "first_known_kwh",
        "running_count",
        "read_at_percent_rank",
        "read_at_to_date_quality_sum_voltage",
        "last_read_at_hour",
        "max_read_at_hour_so_far_share_of_read_at_seq",
        "read_at_seq_avg_2",
        "min_last_read_at_hour_so_far",
        "first_known_read_at_to_date_max_read_at_percent_",
        "rows_in_first_known_read_at_to_date",
        F.date_format(F.col("first_known_read_at"), "yyyy-MM").alias("first_known_read_at_label"),
    )
    .orderBy(F.col("read_at_seq_avg_2").asc())
)
