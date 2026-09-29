from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_track_duratio = Window.partitionBy("listener_id").orderBy(F.col("track_duration_sec").desc(), "id")
w_is_skipped_by_track_duration = Window.partitionBy("is_skipped").orderBy("track_duration_sec", "id")
w_is_skipped_by_ms_played_runn = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_played_at_month_source_or_un = Window.partitionBy(F.month("played_at"), "source_or_unknown")
w_listener_signup_date_month_i = Window.partitionBy(F.month("listener_signup_date"), "is_skipped")
w_source_or_unknown_is_skipped = Window.partitionBy("source_or_unknown", "is_skipped")
w_listener_id_source_or_unknow = Window.partitionBy("listener_id", "source_or_unknown").orderBy(F.col("ms_played").desc(), "id")
w_source_or_unknown_by_ms_play = Window.partitionBy("source_or_unknown").orderBy("ms_played")
w_source_or_unknown_by_ms_play_2 = Window.partitionBy("source_or_unknown").orderBy("ms_played", "id")
w_all_by_played_at_ties = Window.orderBy("played_at")

scoped_plays = (
    df_plays
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

enriched = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source_or_unknown",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.round(F.percent_rank().over(w_listener_id_by_track_duratio), 3).alias("track_duration_sec_percent_rank"),
        F.lead("track_duration_sec", 3).over(w_is_skipped_by_track_duration).alias("next3_track_duration_sec"),
        F.sum("track_duration_sec").over(w_is_skipped_by_ms_played_runn).alias("running_track_duration_sec"),
        F.ntile(3).over(w_is_skipped_by_ms_played).alias("ms_played_tercile"),
        F.max("ms_played").over(w_played_at_month_source_or_un).alias("played_at_month_source_or_unknown_max_ms_played"),
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_listener_signup_date_month_i), 2).alias("track_duration_sec_vs_listener_signup_date_month"),
        F.max("ms_played").over(w_source_or_unknown_is_skipped).alias("source_or_unknown_is_skipped_max_ms_played"),
        F.round(F.cume_dist().over(w_listener_id_source_or_unknow), 3).alias("ms_played_cume_dist"),
        F.dense_rank().over(w_source_or_unknown_by_ms_play).alias("ms_played_dense_rank"),
        F.rank().over(w_source_or_unknown_by_ms_play).alias("ms_played_rank"),
        F.size(F.collect_set("track_id").over(w_source_or_unknown_is_skipped)).alias("distinct_track_id_in_source_or_unknown_is_skippe"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_signup_date_month_i), 2).alias("ms_played_vs_listener_signup_date_month_is_skipp"),
        F.ntile(3).over(w_source_or_unknown_by_ms_play_2).alias("ms_played_tercile_2"),
        F.count(F.lit(1)).over(w_played_at_month_source_or_un).alias("rows_in_played_at_month_source_or_unknown"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_month_source_or_un), F.lit(0)), 4).alias("ms_played_share_of_played_at_month_source_or_unk"),
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
    )
)

stats = (
    with_trends
    .groupBy("ms_played_rank", "is_skipped")
    .agg(
        F.count(F.when(F.lower(F.col("track_title")).like("%gh%"), 1)).alias("n_lower_track_title_like_gh"),
        F.round(F.sum("track_duration_sec_vs_listener_signup_date_month"), 2).alias("total_track_duration_sec_vs_listener_signup_date"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    stats
    .select(
        "ms_played_rank",
        "is_skipped",
        "n_lower_track_title_like_gh",
        "total_track_duration_sec_vs_listener_signup_date",
        "n_rows",
        F.round(F.col("ms_played_rank") / F.nullif(F.col("n_rows"), F.lit(0)), 3).alias("ms_played_rank_per_n_rows"),
        F.greatest(F.col("ms_played_rank"), F.col("n_rows")).alias("greatest_ms_played_rank_n_rows"),
    )
    .orderBy(F.col("n_rows").asc())
)
