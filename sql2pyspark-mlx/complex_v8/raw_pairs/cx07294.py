from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id").orderBy("played_at", "id")
w_track_id_running = Window.partitionBy("track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_starts_session_tie = Window.partitionBy("device").orderBy(F.col("starts_session").desc())
w_all_by_ms_played_bucket = Window.orderBy(F.col("ms_played_bucket").desc_nulls_first(), "id")
w_device = Window.partitionBy("device")

valid_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
    )
)

with_prev = valid_plays.withColumn("prev_played_at", F.lag("played_at").over(w_track_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_played_at").isNull() | ((F.unix_timestamp("played_at") - F.unix_timestamp("prev_played_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_track_id_running))

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source") == "search", 1)).alias("play_source_count"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    sessionized.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_trends = (
    summarized_rows
    .select(
        "*",
        F.round(F.percent_rank().over(w_device_by_starts_session_tie), 3).alias("starts_session_percent_rank"),
        F.row_number().over(w_all_by_ms_played_bucket).alias("seq_no"),
        F.lead("play_source_count", 2).over(w_all_by_ms_played_bucket).alias("next2_play_source_count"),
        F.min("play_count").over(w_device).alias("device_min_play_count"),
        F.round(F.avg("play_count").over(w_device), 2).alias("device_avg_play_count"),
    )
)

result = (
    with_trends
    .select("id", "starts_session_percent_rank", "device_avg_play_count")
    .unpivot(
        ids=["id"],
        values=["starts_session_percent_rank", "device_avg_play_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
