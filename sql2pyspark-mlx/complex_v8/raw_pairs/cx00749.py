from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_play_ms_played_ties = Window.orderBy("play_ms_played")
w_artist_id_play_device_2 = Window.partitionBy("artist_id", "play_device_2")

prepared = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("duration_sec") - 312).alias("duration_sec_distance"),
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
    )
)

exploded_detail = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

with_details_artist = (
    exploded_detail.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
        F.col("p.device").alias("play_device_2"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

combined_details = (
    with_details_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year_2"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        "play_id",
        "play_played_at",
        "play_device",
        "artist_debut_year",
        "artist_country",
        "play_ms_played",
        "artist_genre",
        "artist_debut_year_2",
        F.round(F.percent_rank().over(w_all_by_play_ms_played_ties), 3).alias("play_ms_played_percent_rank"),
        F.size(F.collect_set("play_device").over(w_artist_id_play_device_2)).alias("distinct_play_device_in_artist_id_play_device_2"),
        F.round(F.col("artist_debut_year_2") - F.avg("artist_debut_year_2").over(w_artist_id_play_device_2), 2).alias("artist_debut_year_2_vs_artist_id_play_device_2_a"),
        F.sum("artist_debut_year").over(w_artist_id_play_device_2).alias("artist_id_play_device_2_sum_artist_debut_year"),
    )
)

scored = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "play_id",
        "play_played_at",
        "artist_debut_year",
        "artist_country",
        "play_ms_played",
        "artist_genre",
        "play_ms_played_percent_rank",
        "distinct_play_device_in_artist_id_play_device_2",
        "artist_id_play_device_2_sum_artist_debut_year",
        F.when(F.col("duration_sec") >= 357, "A").when(F.col("duration_sec") >= 213, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

result = (
    scored
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "play_id",
        "play_played_at",
        "artist_country",
        "play_ms_played",
        "artist_genre",
        "distinct_play_device_in_artist_id_play_device_2",
        "duration_sec_tier",
        F.lower(F.regexp_replace(F.trim(F.col("artist_country")), " ", "_")).alias("artist_country_slug"),
    )
)
