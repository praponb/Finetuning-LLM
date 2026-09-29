from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec_filled", "id")
w_is_explicit_by_bpm_moving_3 = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_all_by_track_release_date_mo = Window.orderBy("track_release_date", "id", "play_id").rowsBetween(-6, Window.currentRow)
w_all_by_track_release_date = Window.orderBy("track_release_date", "id", "play_id")
w_all_by_track_release_date_ru = Window.orderBy("track_release_date", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_track_release_date_pr = Window.orderBy("track_release_date", "id", "play_id").rowsBetween(-1, -1)

prepared = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.sum("bpm").over(w_is_explicit).alias("is_explicit_sum_bpm"),
        F.sum("duration_sec").over(w_is_explicit_by_duration_sec).alias("running_duration_sec"),
        F.lag("release_date_month").over(w_is_explicit_by_duration_sec_2).alias("prev_release_date_month"),
        F.round(F.avg("duration_sec_filled").over(w_is_explicit_by_bpm_moving_3), 2).alias("duration_sec_filled_avg_4"),
    )
)

line_level = (
    windowed.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.duration_sec_filled",
        "s.release_date_month",
        "s.running_duration_sec",
        "s.prev_release_date_month",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

enriched = (
    line_level.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.release_date_month",
        "s.running_duration_sec",
        "s.prev_release_date_month",
        "s.play_id",
        "s.play_played_at",
        "s.play_ms_played",
        "s.play_listener_id",
        F.col("p.ms_played").alias("play_ms_played_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.source").alias("play_source"),
        "a.artist_name",
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.play_track_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.running_duration_sec",
        "s.prev_release_date_month",
        "s.play_id",
        "s.play_played_at",
        "s.play_ms_played",
        "s.play_listener_id",
        "s.play_ms_played_2",
        "s.play_track_id",
        "s.artist_name",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "prev_release_date_month",
        "play_id",
        "play_ms_played",
        "play_listener_id",
        "artist_name",
        "track_release_date",
        "track_is_explicit",
        F.round(F.avg("play_ms_played_2").over(w_all_by_track_release_date_mo), 2).alias("play_ms_played_2_avg_7"),
        F.lead("running_duration_sec").over(w_all_by_track_release_date).alias("next_running_duration_sec"),
        (F.col("play_ms_played") - F.lag("play_ms_played").over(w_all_by_track_release_date)).alias("play_ms_played_change"),
        F.min("play_ms_played").over(w_all_by_track_release_date_ru).alias("min_play_ms_played_so_far"),
        F.row_number().over(w_all_by_track_release_date).alias("seq_no"),
        F.sum("running_duration_sec").over(w_all_by_track_release_date_pr).alias("running_duration_sec_sum_1_prior"),
    )
)

stats = (
    with_windows
    .groupBy(
        F.when(F.col("play_ms_played") >= 239955, "large").when(F.col("play_ms_played") >= 129336, "medium").otherwise("small").alias("play_ms_played_tier"),
        "is_explicit",
        "artist_id",
    )
    .agg(
        F.sum("running_duration_sec_sum_1_prior").alias("total_running_duration_sec_sum_1_prior"),
        F.sum("play_ms_played_change").alias("total_play_ms_played_change"),
        F.max("next_running_duration_sec").alias("max_next_running_duration_sec"),
        F.datediff(F.max("track_release_date"), F.min("track_release_date")).alias("track_release_date_span_days"),
        F.count(F.when(~F.col("track_is_explicit"), 1)).alias("n_not_track_is_explicit"),
    )
)

result = (
    stats
    .select(
        "play_ms_played_tier",
        "is_explicit",
        F.col("track_release_date_span_days").alias("m_track_release_date_span_days"),
        F.col("total_running_duration_sec_sum_1_prior").alias("m_total_running_duration_sec_sum_1_prior"),
        F.col("n_not_track_is_explicit").alias("n_not_track_is_explicit_v"),
    )
    .unpivot(
        ids=["play_ms_played_tier", "is_explicit"],
        values=["m_track_release_date_span_days", "m_total_running_duration_sec_sum_1_prior", "n_not_track_is_explicit_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("play_ms_played_tier", "is_explicit", "field")
)
