from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_source_count_by_duratio = Window.partitionBy("play_source_count").orderBy(F.col("duration_sec_per_bpm").desc(), "id")
w_play_source_count_release_da = Window.partitionBy("play_source_count", F.month("release_date"))
w_play_id_by_play_ms_played_ti = Window.partitionBy("play_id").orderBy(F.col("play_ms_played").desc_nulls_first())
w_is_explicit_play_device_by_p = Window.partitionBy("is_explicit", "play_device").orderBy("play_played_at", "id", "play_id")
w_is_explicit_play_device_by_p_2 = Window.partitionBy("is_explicit", "play_device").orderBy("play_played_at")
w_play_id_by_play_ms_played = Window.partitionBy("play_id").orderBy(F.col("play_ms_played").desc_nulls_first(), "id", "play_id")

valid = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%bu%")) & (F.year(F.col("release_date")) == 2025))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("source") == "playlist", 1)).alias("play_source_count"),
    )
)

summarized_rows = (
    valid.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

with_windows = (
    summarized_rows
    .select(
        "*",
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_play_source_count_by_duratio)).alias("days_since_prev_release_date"),
        F.ntile(4).over(w_play_source_count_by_duratio).alias("duration_sec_per_bpm_quartile"),
        F.count(F.lit(1)).over(w_play_source_count_release_da).alias("rows_in_play_source_count_release_date_month"),
        F.round(F.col("play_source_count") / F.nullif(F.sum("play_source_count").over(w_play_source_count_release_da), F.lit(0)), 4).alias("play_source_count_share_of_play_source_count_rel"),
        (F.col("duration_sec_per_bpm") - F.lag("duration_sec_per_bpm").over(w_play_source_count_by_duratio)).alias("duration_sec_per_bpm_change"),
    )
)

line_level_plays = (
    with_windows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.duration_sec_per_bpm",
        "s.title_prefix",
        "s.duration_sec_per_bpm_quartile",
        "s.rows_in_play_source_count_release_date_month",
        "s.play_source_count_share_of_play_source_count_rel",
        "s.duration_sec_per_bpm_change",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_trends = (
    line_level_plays
    .select(
        "id",
        "title_prefix",
        "duration_sec_per_bpm_quartile",
        "rows_in_play_source_count_release_date_month",
        "duration_sec_per_bpm_change",
        "play_id",
        "play_source",
        "play_ms_played",
        F.rank().over(w_play_id_by_play_ms_played_ti).alias("play_ms_played_rank"),
        F.row_number().over(w_is_explicit_play_device_by_p).alias("seq_no"),
        F.dense_rank().over(w_is_explicit_play_device_by_p_2).alias("play_played_at_dense_rank"),
        F.lead("duration_sec_per_bpm", 2).over(w_play_id_by_play_ms_played).alias("next2_duration_sec_per_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_play_device_by_p), 3).alias("play_played_at_percent_rank"),
    )
)

with_parent_play = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.id") == F.col("s.play_id")) & (~F.col("p.is_skipped")), "inner")
    .select("s.*", F.col("p.listener_id").alias("play_listener_id"))
)

result = (
    with_parent_play
    .cube("play_id", "play_source", "play_ms_played_rank")
    .agg(
        F.min("next2_duration_sec_per_bpm").alias("min_next2_duration_sec_per_bpm"),
        F.count(F.when(F.col("play_source").isin("search", "radio", "playlist"), 1)).alias("n_play_source_in_search_radio"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("play_id").asc_nulls_last(),
        F.col("play_source").asc_nulls_last(),
        F.col("play_ms_played_rank").asc_nulls_last(),
    )
)
