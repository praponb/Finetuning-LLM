from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_all_by_voltage = Window.orderBy(F.col("voltage").desc(), "id")
w_all_by_kwh_ties = Window.orderBy("kwh")
w_all_by_kwh_full = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_site_id_quality_by_kwh = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_kwh = Window.orderBy("kwh", "id")
w_site_id_kwh_rank_by_kwh_cume = Window.partitionBy("site_id", "kwh_rank").orderBy("kwh_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prev3_read_at_month_seq_no_b = Window.partitionBy(F.date_trunc("month", "prev3_read_at"), "seq_no").orderBy(F.col("kwh").desc(), "id")
w_site_id_first_known_read_at = Window.partitionBy("site_id", F.year("first_known_read_at"))
w_kwh_rank_by_voltage_ties = Window.partitionBy("kwh_rank").orderBy(F.col("voltage").desc())
w_prev3_read_at_month_seq_no_b_2 = Window.partitionBy(F.date_trunc("month", "prev3_read_at"), "seq_no").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_read_at_date_month_voltage_d = Window.partitionBy(F.date_trunc("month", "read_at_date"), "voltage_dense_rank").orderBy(F.col("voltage_cume_dist").desc_nulls_first(), "id")
w_site_id_kwh_rank_by_kwh_cume_2 = Window.partitionBy("site_id", "kwh_rank").orderBy("kwh_cume_dist", "id")
w_prev_read_at_year_kwh_dense = Window.partitionBy(F.year("prev_read_at"), "kwh_dense_rank")

scoped_readings = (
    df_meter_readings
    .filter(F.col("read_at") < F.lit("2025-02-12 14:00:00").cast("timestamp"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

sequenced = (
    scoped_readings
    .select(
        "*",
        F.sum("kwh").over(w_quality_site_id).alias("quality_site_id_sum_kwh"),
        F.dense_rank().over(w_all_by_voltage).alias("voltage_dense_rank"),
        F.dense_rank().over(w_all_by_kwh_ties).alias("kwh_dense_rank"),
        F.lag("read_at", 3).over(w_all_by_voltage).alias("prev3_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.last("voltage").over(w_all_by_kwh_full).alias("last_voltage"),
        F.round(F.cume_dist().over(w_all_by_voltage), 3).alias("voltage_cume_dist"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_site_id_quality_by_kwh))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.first("read_at", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_read_at"),
        F.round(F.cume_dist().over(w_all_by_kwh), 3).alias("kwh_cume_dist"),
        F.rank().over(w_all_by_kwh_ties).alias("kwh_rank"),
        F.row_number().over(w_all_by_voltage).alias("seq_no"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_site_id), 2).alias("voltage_vs_quality_site_id_avg"),
        F.lag("read_at").over(w_all_by_voltage).alias("prev_read_at"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_all_by_voltage))) / 60, 1).alias("minutes_since_prev_read_at_2"),
        F.round(F.percent_rank().over(w_site_id_quality_by_kwh), 3).alias("kwh_percent_rank"),
    )
)

metrics = (
    sequenced
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "read_at_date",
        "quality_site_id_sum_kwh",
        "prev3_read_at",
        "kwh_vs_quality_site_id_avg",
        "last_voltage",
        "first_known_read_at",
        "kwh_cume_dist",
        "kwh_rank",
        "seq_no",
        "voltage_vs_quality_site_id_avg",
        "prev_read_at",
        "minutes_since_prev_read_at_2",
        F.last("kwh_rank").over(w_site_id_kwh_rank_by_kwh_cume).alias("last_kwh_rank"),
        F.lead("kwh_vs_quality_site_id_avg").over(w_prev3_read_at_month_seq_no_b).alias("next_kwh_vs_quality_site_id_avg"),
        F.min("kwh_dense_rank").over(w_site_id_first_known_read_at).alias("site_id_first_known_read_at_year_min_kwh_dense_r"),
        F.rank().over(w_kwh_rank_by_voltage_ties).alias("voltage_rank"),
        F.nth_value("quality_site_id_sum_kwh", 2).over(w_prev3_read_at_month_seq_no_b_2).alias("second_quality_site_id_sum_kwh"),
        F.row_number().over(w_read_at_date_month_voltage_d).alias("rn"),
        F.ntile(5).over(w_site_id_kwh_rank_by_kwh_cume_2).alias("kwh_cume_dist_quintile"),
        F.count(F.lit(1)).over(w_prev_read_at_year_kwh_dense).alias("rows_in_prev_read_at_year_kwh_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_date",
        "quality_site_id_sum_kwh",
        "prev3_read_at",
        "kwh_vs_quality_site_id_avg",
        "last_voltage",
        "kwh_cume_dist",
        "kwh_rank",
        "seq_no",
        "voltage_vs_quality_site_id_avg",
        "prev_read_at",
        "minutes_since_prev_read_at_2",
        "next_kwh_vs_quality_site_id_avg",
        "site_id_first_known_read_at_year_min_kwh_dense_r",
        "voltage_rank",
        "rn",
        F.when(F.col("kwh_cume_dist") >= 0.6, "large").when(F.col("kwh_cume_dist") >= 0.4, "medium").otherwise("small").alias("kwh_cume_dist_tier"),
        F.concat(F.col("quality"), F.lit("#"), F.col("voltage_rank").cast("string")).alias("quality_ref"),
    )
    .orderBy(F.col("kwh_cume_dist").desc())
)
