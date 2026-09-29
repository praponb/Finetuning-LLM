from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("play_device", "artist_id").orderBy(F.col("release_date").desc(), F.col("id").desc(), F.col("play_id").desc())

prepared_tracks = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%pa%"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_activity = (
    prepared_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id").isNotNull()), "left_semi")
    .filter(F.array_contains(F.col("moods"), "chill"))
)

exploded_detail = (
    with_activity.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.title_clean",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

latest = (
    exploded_detail
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail_plays = (
    latest.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.title_clean",
        "s.play_id",
        "s.play_listener_id",
        "s.play_played_at",
        F.col("p.id").alias("play_id_2"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    exploded_detail_plays
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "title_clean",
        "play_id",
        "play_played_at",
        "play_id_2",
        "play_ms_played",
        "play_played_at_2",
        "play_source",
        F.when(F.col("play_source").isin("radio", "album"), "group_a").when(F.col("play_source") == "playlist", "group_b").otherwise("other").alias("play_source_group"),
    )
    .orderBy(F.col("play_source").desc(), F.col("play_ms_played").desc_nulls_first())
)
