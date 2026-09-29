from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_source_by_play_ms_playe = Window.partitionBy("play_source").orderBy("play_ms_played", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_source = Window.partitionBy("play_source")
w_last_played_at_year_by_first = Window.partitionBy(F.year("last_played_at")).orderBy("first_played_at_2", "id", "play_id")
w_last_played_at_year_by_first_2 = Window.partitionBy(F.year("last_played_at")).orderBy("first_played_at_2", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_play_ms_played_prior = Window.orderBy(F.col("play_ms_played").desc_nulls_first(), "id", "play_id").rowsBetween(-1, -1)

filtered = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 155, "large").when(F.col("bpm") >= 82, "medium").otherwise("small").alias("bpm_tier"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.min("played_at").alias("first_played_at_2"),
    )
)

profiled = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.first_played_at_2")
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

summarized_rows = (
    profiled.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

exploded_detail_plays = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.bpm_tier",
        "s.release_date_label",
        "s.title_slug",
        "s.first_played_at_2",
        "s.last_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
    )
)

with_windows = (
    exploded_detail_plays
    .select(
        "id",
        "moods",
        "bpm_tier",
        "title_slug",
        "first_played_at_2",
        "last_played_at",
        "play_id",
        "play_is_skipped",
        "play_source",
        F.last("last_played_at").over(w_play_source_by_play_ms_playe).alias("last_last_played_at"),
        F.size(F.collect_set("bpm_tier").over(w_play_source)).alias("distinct_bpm_tier_in_play_source"),
        F.round((F.unix_timestamp("first_played_at_2") - F.unix_timestamp(F.lag("first_played_at_2").over(w_last_played_at_year_by_first))) / 60, 1).alias("minutes_since_prev_first_played_at_2"),
        F.last("play_ms_played", ignorenulls=True).over(w_last_played_at_year_by_first_2).alias("last_known_play_ms_played"),
        F.sum("play_ms_played").over(w_all_by_play_ms_played_prior).alias("play_ms_played_sum_1_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "play_id",
        "play_ms_played_sum_1_prior",
        "last_known_play_ms_played",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["play_ms_played_sum_1_prior", "last_known_play_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
