from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("play_listener_id", "play_device").orderBy(F.col("avg_duration_sec").desc(), F.col("id").desc(), F.col("play_id").desc())

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

line_level = (
    recent_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.is_explicit_flag",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

play_device_stats = (
    line_level
    .groupBy("play_device")
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)

compared = (
    line_level.alias("s")
    .join(play_device_stats.alias("g"), F.col("g.play_device") == F.col("s.play_device"), "inner")
    .filter(F.col("s.duration_sec") > F.col("g.avg_duration_sec"))
    .select(
        "s.*",
        "g.avg_duration_sec",
        F.round(F.col("s.duration_sec") - F.col("g.avg_duration_sec"), 2).alias("duration_sec_minus_avg"),
    )
)

latest = (
    compared
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .groupingSets(
        [["artist_id", "is_explicit_flag", "play_listener_id"], ["artist_id"]],
        "artist_id", "is_explicit_flag", "play_listener_id",
    )
    .agg(
        F.min("play_played_at").alias("first_play_played_at"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("play_device") == "speaker", F.col("duration_sec_minus_avg"))).alias("duration_sec_minus_avg_play_device_speaker"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit_flag").asc_nulls_last(),
        F.col("play_listener_id").asc_nulls_last(),
    )
)
