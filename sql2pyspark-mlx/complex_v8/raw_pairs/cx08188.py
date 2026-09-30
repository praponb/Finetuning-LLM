from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played_full = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_played_at_month_by_pl = Window.partitionBy("device", F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_ms_played_prior_3 = Window.partitionBy("device").orderBy("ms_played", "id").rowsBetween(-3, -1)
w_track_id_by_played_at_runnin = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_played_at = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_track_id_by_played_at_ties = Window.partitionBy("track_id").orderBy(F.col("played_at").desc())
w_listener_id_device_by_first = Window.partitionBy("listener_id", "device").orderBy(F.col("first_known_played_at").desc(), "id")
w_first_known_played_at_month = Window.partitionBy(F.date_trunc("month", "first_known_played_at")).orderBy("second_played_at", "id")
w_played_at_seq_device = Window.partitionBy("played_at_seq", "device")

base_plays = (
    df_plays
    .filter((F.col("source").isin("album", "playlist")) & (F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.initcap(F.trim(F.col("source"))).alias("source_clean"),
    )
)

with_windows = (
    base_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "source_clean",
        F.nth_value("played_at", 2).over(w_device_by_ms_played_full).alias("second_played_at"),
        F.last("ms_played").over(w_device_played_at_month_by_pl).alias("last_ms_played"),
        F.first("played_at", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_played_at"),
        F.max("ms_played").over(w_device_by_ms_played_prior_3).alias("ms_played_max_3_prior"),
        F.last("ms_played", ignorenulls=True).over(w_track_id_by_played_at_runnin).alias("last_known_ms_played"),
        F.row_number().over(w_track_id_by_played_at).alias("played_at_seq"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.round(F.percent_rank().over(w_track_id_by_played_at_ties), 3).alias("played_at_percent_rank"),
    )
)

annotated = with_windows.select("*", F.initcap(F.trim(F.col("device"))).alias("device_clean"))

ranked = (
    annotated
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "source_clean",
        "second_played_at",
        "last_ms_played",
        "played_at_seq",
        "played_at_percent_rank",
        F.lead("played_at_seq").over(w_listener_id_device_by_first).alias("next_played_at_seq"),
        F.round(F.percent_rank().over(w_first_known_played_at_month), 3).alias("second_played_at_percent_rank"),
        F.round(F.avg("played_at_seq").over(w_played_at_seq_device), 2).alias("played_at_seq_device_avg_played_at_seq"),
        F.count(F.lit(1)).over(w_played_at_seq_device).alias("rows_in_played_at_seq_device"),
        F.ntile(10).over(w_listener_id_device_by_first).alias("first_known_played_at_decile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "second_played_at",
        "played_at_seq",
        "played_at_percent_rank",
        "next_played_at_seq",
        "played_at_seq_device_avg_played_at_seq",
        "rows_in_played_at_seq_device",
        "first_known_played_at_decile",
        F.round(F.col("played_at_percent_rank") * 0.9, 2).alias("played_at_percent_rank_adj"),
    )
    .orderBy(F.col("played_at_seq").desc())
)
