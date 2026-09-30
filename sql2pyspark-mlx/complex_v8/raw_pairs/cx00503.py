from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_genre_by_le = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("least_duration_sec_duration_sec_filled").asc_nulls_last())
w_artist_id_artist_genre_by_le_2 = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("least_duration_sec_duration_sec_filled").asc_nulls_last(), "id")
w_all_by_least_duration_sec_du = Window.orderBy(F.col("least_duration_sec_duration_sec_filled").desc())

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") >= 239)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.when(F.col("bpm") >= 147, "large").when(F.col("bpm") >= 85, "medium").otherwise("small").alias("bpm_tier"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

derived = (
    cleaned_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm_tier",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.least(F.col("duration_sec"), F.col("duration_sec_filled")).alias("least_duration_sec_duration_sec_filled"),
    )
)

enriched_artist = (
    derived.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isNotNull())
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm_tier",
        "s.release_date_label",
        "s.least_duration_sec_duration_sec_filled",
        F.col("a.genre").alias("artist_genre"),
    )
)

ranked = (
    enriched_artist
    .select(
        "*",
        F.rank().over(w_artist_id_artist_genre_by_le).alias("least_duration_sec_duration_sec_filled_rank"),
        F.row_number().over(w_artist_id_artist_genre_by_le_2).alias("least_duration_sec_duration_sec_filled_seq"),
        F.round(F.percent_rank().over(w_all_by_least_duration_sec_du), 3).alias("least_duration_sec_duration_sec_filled_percent_r"),
        F.rank().over(w_artist_id_artist_genre_by_le_2).alias("least_duration_sec_duration_sec_filled_rank_2"),
        F.round(F.cume_dist().over(w_artist_id_artist_genre_by_le_2), 3).alias("least_duration_sec_duration_sec_filled_cume_dist"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

with_stats = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.total_play_ms_played",
    )
)

enriched = (
    with_stats
    .filter(F.col("least_duration_sec_duration_sec_filled_cume_dist") > 0.2)
    .withColumn("total_play_ms_played_filled", F.coalesce(F.col("total_play_ms_played"), F.lit(0)))
)

derived_2 = (
    enriched
    .select(
        "*",
        F.concat_ws(" / ", F.col("artist_genre"), F.col("title")).alias("artist_genre_title_label"),
        F.round(F.col("total_play_ms_played") / F.nullif(F.col("play_count"), F.lit(0)), 3).alias("total_play_ms_played_per_play_count"),
    )
)

result = (
    derived_2
    .cube("is_explicit", "play_count", "artist_id")
    .agg(F.count("*").alias("row_count"))
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("play_count").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
