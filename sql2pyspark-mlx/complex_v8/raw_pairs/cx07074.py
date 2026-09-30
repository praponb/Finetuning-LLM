from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year_track_id_by_p = Window.partitionBy(F.year("played_at"), "track_id").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(-2, -1)
w_device_source = Window.partitionBy("device", "source")
w_source_played_at_month_by_pl = Window.partitionBy("source", F.month("played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_source_by_played_at_quarter = Window.partitionBy("source").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(-1, Window.currentRow)
w_listener_id_played_at_quarte = Window.partitionBy("listener_id", F.month("played_at_quarter"))
w_source_by_played_at_quarter_2 = Window.partitionBy("source").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_played_at_month_by_pl_2 = Window.partitionBy("source", F.month("played_at")).orderBy("played_at", "id")
w_source_by_played_at_quarter_3 = Window.partitionBy("source").orderBy(F.col("played_at_quarter").desc(), "id")
w_source_played_at_month_by_pl_3 = Window.partitionBy("source", F.month("played_at")).orderBy("played_at")
w_all_by_played_at_quarter_mov = Window.orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_played_at_quarter_run = Window.orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.max("ms_played").over(w_played_at_year_track_id_by_p).alias("ms_played_max_2_prior"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_device_source), F.lit(0)), 4).alias("ms_played_share_of_device_source"),
        F.sum("ms_played").over(w_source_played_at_month_by_pl).alias("running_ms_played"),
        F.lag("played_at", 2).over(w_all_by_played_at).alias("prev2_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_device_source), 2).alias("ms_played_vs_device_source_avg"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
        F.round(F.avg("ms_played").over(w_source_by_played_at_quarter), 2).alias("ms_played_avg_2"),
        F.count(F.lit(1)).over(w_listener_id_played_at_quarte).alias("rows_in_listener_id_played_at_quarter_month"),
        F.last("ms_played").over(w_source_by_played_at_quarter_2).alias("last_ms_played"),
        F.ntile(5).over(w_source_played_at_month_by_pl_2).alias("played_at_quintile"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_by_played_at_quarter_3)).alias("ms_played_change"),
        F.round(F.cume_dist().over(w_source_played_at_month_by_pl_3), 3).alias("played_at_cume_dist"),
        F.sum("ms_played").over(w_all_by_played_at_quarter_mov).alias("ms_played_sum_7"),
        F.min("ms_played").over(w_all_by_played_at_quarter_run).alias("min_ms_played_so_far"),
    )
)

tagged = (
    with_trends
    .select(
        "*",
        F.concat_ws("-", F.col("source"), F.col("device")).alias("source_device_label"),
        F.round((F.unix_timestamp(F.col("played_at")) - F.unix_timestamp(F.col("played_at_quarter"))) / 3600, 1).alias("hours_played_at_quarter_to_played_at"),
        F.to_date(F.col("prev2_played_at")).alias("prev2_played_at_date"),
    )
)

result = (
    tagged
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "played_at_label",
        "ms_played_share_of_device_source",
        "prev2_played_at",
        "ms_played_vs_device_source_avg",
        "rn",
        "rows_in_listener_id_played_at_quarter_month",
        "last_ms_played",
        "played_at_quintile",
        "ms_played_change",
        "played_at_cume_dist",
        "ms_played_sum_7",
        "prev2_played_at_date",
    )
    .orderBy("id", F.col("ms_played_vs_device_source_avg").desc())
    .limit(10)
)
