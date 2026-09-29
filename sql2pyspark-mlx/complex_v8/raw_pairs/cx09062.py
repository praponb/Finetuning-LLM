from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_read_at_ties = Window.orderBy(F.col("read_at").desc())
w_quality_len_site_id_by_quali = Window.partitionBy("quality_len", "site_id").orderBy("quality_len", "id")
w_site_id_by_read_at_ties = Window.partitionBy("site_id").orderBy("read_at")
w_quality_len = Window.partitionBy("quality_len")
w_all_by_read_at_running = Window.orderBy(F.col("read_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_read_at_to_date_quality_len = Window.partitionBy(F.to_date("read_at"), "quality_len").orderBy("quality_len")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")
w_read_at_to_date = Window.partitionBy(F.to_date("read_at"))
w_quality_len_by_prev_quality = Window.partitionBy("quality_len").orderBy("prev_quality_len", "id").rowsBetween(-6, -1)

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality_prefix",
        "quality_len",
        F.round(F.cume_dist().over(w_all_by_read_at_ties), 3).alias("read_at_cume_dist"),
        F.lag("kwh").over(w_quality_len_site_id_by_quali).alias("prev_kwh"),
        F.round(F.cume_dist().over(w_site_id_by_read_at_ties), 3).alias("read_at_cume_dist_2"),
        F.sum("voltage").over(w_quality_len).alias("quality_len_sum_voltage"),
        F.first("voltage", ignorenulls=True).over(w_all_by_read_at_running).alias("first_known_voltage"),
        F.round(F.col("quality_len") / F.nullif(F.sum("quality_len").over(w_quality_len), F.lit(0)), 4).alias("quality_len_share_of_quality_len"),
        F.round(F.percent_rank().over(w_read_at_to_date_quality_len), 3).alias("quality_len_percent_rank"),
        F.lag("quality_len", 1, 0).over(w_site_id_by_read_at).alias("prev_quality_len"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "read_at",
        "kwh",
        "quality_prefix",
        "quality_len",
        "read_at_cume_dist_2",
        "first_known_voltage",
        "quality_len_percent_rank",
        F.round(F.col("quality_len_percent_rank") / F.nullif(F.sum("quality_len_percent_rank").over(w_read_at_to_date), F.lit(0)), 4).alias("quality_len_percent_rank_share_of_read_at_to_dat"),
        F.max("read_at_cume_dist_2").over(w_quality_len_by_prev_quality).alias("read_at_cume_dist_2_max_6_prior"),
        F.count(F.lit(1)).over(w_read_at_to_date).alias("rows_in_read_at_to_date"),
        F.round(F.col("prev_quality_len") - F.avg("prev_quality_len").over(w_read_at_to_date), 2).alias("prev_quality_len_vs_read_at_to_date_avg"),
    )
)

rollup_base = (
    with_windows_2
    .groupBy(F.date_trunc("month", "read_at").alias("read_at_month"), "quality_len")
    .agg(
        F.min("read_at").alias("first_read_at"),
        F.round(F.stddev("quality_len_percent_rank"), 2).alias("stddev_quality_len_percent_rank"),
        F.count("*").alias("row_count"),
        F.sum("kwh").alias("total_kwh"),
        F.sum("quality_len_percent_rank_share_of_read_at_to_dat").alias("sum_quality_len_percent_rank_share_of_read_at_to"),
    )
    .filter(F.col("sum_quality_len_percent_rank_share_of_read_at_to") > 0.1)
    .drop("sum_quality_len_percent_rank_share_of_read_at_to")
)

result = (
    rollup_base
    .filter(F.col("row_count") <= 10)
    .select(
        "read_at_month",
        "quality_len",
        "first_read_at",
        "stddev_quality_len_percent_rank",
        "row_count",
        "total_kwh",
        F.round(F.col("total_kwh") * 1.1, 2).alias("total_kwh_adj"),
        F.round(F.col("total_kwh") * 0.25, 2).alias("total_kwh_adj_2"),
    )
    .orderBy(F.col("row_count").desc())
)
