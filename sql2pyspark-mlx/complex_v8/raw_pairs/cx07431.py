from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")

prepared = (
    df_tracks
    .filter(F.col("bpm").between(85, 143))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.nth_value("release_date", 2).over(w_is_explicit_by_bpm_full).alias("second_release_date"),
        F.lead("bpm").over(w_is_explicit_by_bpm).alias("next_bpm"),
        F.lead("duration_sec", 1, 0).over(w_all_by_release_date).alias("next_duration_sec"),
    )
)

flattened = (
    windowed
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "second_release_date",
        "next_bpm",
        "next_duration_sec",
        F.explode("moods").alias("mood"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(F.col("source") == "radio", 1)).alias("play_source_count"),
    )
)

summarized_rows = (
    flattened.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.is_skipped")) & (F.col("p.listener_id").isNotNull()), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "next_duration_sec",
        "mood",
        "last_played_at",
        "play_source_count",
        F.round(F.col("play_source_count") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("play_source_count_per_duration_sec"),
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
    )
    .orderBy(
        F.col("title").desc_nulls_first(),
        F.col("last_played_at").desc_nulls_first(),
    )
)
