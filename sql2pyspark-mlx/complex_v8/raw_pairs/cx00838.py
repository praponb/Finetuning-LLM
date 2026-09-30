from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_play_listener_id = Window.partitionBy("is_explicit", "play_listener_id").orderBy("duration_sec", "id", "mood", "play_id", "play_id_2")
w_play_played_at_year_play_sou = Window.partitionBy(F.year("play_played_at"), "play_source").orderBy(F.col("release_date").desc(), "id", "mood", "play_id", "play_id_2").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-08").cast("date")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 357, "A").when(F.col("duration_sec") >= 164, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

exploded = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "duration_sec_tier",
        F.explode("moods").alias("mood"),
    )
)

detail_rows_plays = (
    exploded.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.mood",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

line_level_plays = (
    detail_rows_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.mood",
        "s.play_id",
        "s.play_ms_played",
        "s.play_source",
        "s.play_listener_id",
        F.col("p.id").alias("play_id_2"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played_2"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

windowed = (
    line_level_plays
    .withColumn("seq_no", F.row_number().over(w_is_explicit_play_listener_id))
    .withColumn("second_release_date", F.nth_value("release_date", 2).over(w_play_played_at_year_play_sou))
)

per_group = (
    windowed
    .groupBy("mood", F.to_date("play_played_at").alias("play_played_at_day"))
    .agg(
        F.countDistinct("play_source").alias("distinct_play_sources"),
        F.countDistinct("play_device").alias("distinct_play_devices"),
        F.round(F.avg("play_ms_played_2"), 2).alias("avg_play_ms_played_2"),
        F.round(F.stddev("play_ms_played"), 2).alias("stddev_play_ms_played"),
        F.max("seq_no").alias("max_seq_no"),
    )
)

result = (
    per_group
    .filter(F.col("avg_play_ms_played_2") >= 84000.0)
    .select(
        "mood",
        "play_played_at_day",
        "distinct_play_sources",
        "distinct_play_devices",
        "avg_play_ms_played_2",
        "stddev_play_ms_played",
        "max_seq_no",
        F.when(F.col("mood").isin("sad", "party"), "group_a").when(F.col("mood") == "workout", "group_b").otherwise("other").alias("mood_group"),
        F.lower(F.regexp_replace(F.trim(F.col("mood")), " ", "_")).alias("mood_slug"),
    )
    .orderBy(
        F.col("distinct_play_sources").desc_nulls_first(),
        F.col("max_seq_no").asc_nulls_last(),
        "mood",
        "play_played_at_day",
    )
    .limit(10)
)
