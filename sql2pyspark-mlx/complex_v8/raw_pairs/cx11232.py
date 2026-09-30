from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_release_da = Window.orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_days_since_release_da_2 = Window.orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")

prepared = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.first("bpm", ignorenulls=True).over(w_all_by_days_since_release_da).alias("first_known_bpm"),
        F.last("duration_sec").over(w_all_by_days_since_release_da_2).alias("last_duration_sec"),
        F.sum("bpm").over(w_artist_id).alias("artist_id_sum_bpm"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Jazz"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "s.first_known_bpm",
        "s.last_duration_sec",
        "s.artist_id_sum_bpm",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_parent_artist = (
    combined_details.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        "s.first_known_bpm",
        "s.last_duration_sec",
        "s.artist_genre",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"), F.count("*").alias("play_count_2"))
)

profiled = (
    with_parent_artist.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

with_parent = (
    profiled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.days_since_release_date",
        "s.last_duration_sec",
        "s.artist_genre",
        "s.artist_is_verified",
        "s.artist_genre_2",
        "s.play_count",
        "s.play_count_2",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.country").alias("artist_country"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "days_since_release_date",
        "last_duration_sec",
        "artist_genre",
        "artist_is_verified",
        "play_count_2",
        "artist_is_verified_2",
        "artist_country",
        (F.col("bpm") - F.col("play_count")).alias("bpm_minus_play_count"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
    .orderBy("artist_country", F.col("artist_id").desc())
)
