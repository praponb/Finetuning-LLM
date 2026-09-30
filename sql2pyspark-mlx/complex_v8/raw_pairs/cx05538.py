from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_site_id = Window.partitionBy("quality", "site_id")
w_quality_by_read_at_full = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_quality = Window.partitionBy("quality")
w_read_at_month_quality_by_kwh = Window.partitionBy(F.date_trunc("month", "read_at"), "quality").orderBy("kwh", "id")
w_read_at_year_by_read_at = Window.partitionBy(F.year("read_at")).orderBy("read_at", "id")
w_quality_site_id_by_kwh = Window.partitionBy("quality", "site_id").orderBy("kwh", "id")
w_kwh_tercile_quality = Window.partitionBy("kwh_tercile", "quality")
w_all_by_kwh_dense_rank_ties = Window.orderBy("kwh_dense_rank")
w_all_by_kwh_dense_rank_moving = Window.orderBy("kwh_dense_rank", "id").rowsBetween(-1, Window.currentRow)
w_site_id_kwh_tercile_by_minut = Window.partitionBy("site_id", "kwh_tercile").orderBy(F.col("minutes_since_prev_read_at").desc())

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_windows = (
    scoped_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        F.sum("kwh").over(w_quality_site_id).alias("quality_site_id_sum_kwh"),
        F.last("read_at").over(w_quality_by_read_at_full).alias("last_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality), 2).alias("kwh_vs_quality_avg"),
        F.lag("read_at").over(w_read_at_month_quality_by_kwh).alias("prev_read_at"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_site_id), 2).alias("kwh_vs_quality_site_id_avg"),
        F.dense_rank().over(w_read_at_month_quality_by_kwh).alias("kwh_dense_rank"),
        F.row_number().over(w_read_at_month_quality_by_kwh).alias("seq_no"),
        F.round((F.unix_timestamp("read_at") - F.unix_timestamp(F.lag("read_at").over(w_read_at_year_by_read_at))) / 60, 1).alias("minutes_since_prev_read_at"),
        F.ntile(3).over(w_quality_site_id_by_kwh).alias("kwh_tercile"),
    )
)

ranked = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("site_id").over(w_kwh_tercile_quality)).alias("distinct_site_id_in_kwh_tercile_quality"),
        F.dense_rank().over(w_all_by_kwh_dense_rank_ties).alias("kwh_dense_rank_dense_rank"),
        F.max("quality_site_id_sum_kwh").over(w_all_by_kwh_dense_rank_moving).alias("quality_site_id_sum_kwh_max_2"),
        F.round(F.col("kwh_vs_quality_avg") / F.nullif(F.sum("kwh_vs_quality_avg").over(w_kwh_tercile_quality), F.lit(0)), 4).alias("kwh_vs_quality_avg_share_of_kwh_tercile_quality"),
        F.rank().over(w_site_id_kwh_tercile_by_minut).alias("minutes_since_prev_read_at_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "quality_site_id_sum_kwh",
        "last_read_at",
        "kwh_vs_quality_avg",
        "prev_read_at",
        "kwh_vs_quality_site_id_avg",
        "distinct_site_id_in_kwh_tercile_quality",
        "quality_site_id_sum_kwh_max_2",
        "minutes_since_prev_read_at_rank",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.coalesce(F.col("kwh_vs_quality_avg"), F.lit(0.0)).alias("kwh_vs_quality_avg_filled"),
    )
    .orderBy(
        F.col("kwh_vs_quality_site_id_avg").asc_nulls_last(),
        F.col("kwh_vs_quality_avg").asc_nulls_last(),
    )
)
