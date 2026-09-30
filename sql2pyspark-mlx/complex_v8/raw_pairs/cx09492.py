from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_per_duratio = Window.partitionBy("artist_id").orderBy(F.col("bpm_per_duration_sec").desc_nulls_first(), "id")
w_is_explicit_flag = Window.partitionBy("is_explicit_flag")

valid_tracks = (
    df_tracks
    .filter(F.col("artist_id") <= 5)
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

ranked = (
    valid_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit_flag",
        "bpm_per_duration_sec",
        F.lag("bpm", 3).over(w_artist_id_by_bpm_per_duratio).alias("prev3_bpm"),
        F.round(F.col("is_explicit_flag") / F.nullif(F.sum("is_explicit_flag").over(w_is_explicit_flag), F.lit(0)), 4).alias("is_explicit_flag_share_of_is_explicit_flag"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("device").isin("car", "tv"), 1)).alias("play_device_count"),
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

summarized_rows = (
    ranked.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
        "p.first_played_at",
        "p.avg_play_ms_played",
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", "a.artist_name", F.col("a.genre").alias("artist_genre"))
)

joined = (
    enriched.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit_flag",
        "s.bpm_per_duration_sec",
        "s.prev3_bpm",
        "s.is_explicit_flag_share_of_is_explicit_flag",
        "s.first_played_at",
        "s.avg_play_ms_played",
        "s.artist_name",
        "s.artist_genre",
        F.col("a.country").alias("artist_country"),
        F.col("a.artist_name").alias("artist_artist_name"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

joined_artist = (
    joined.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified_2"),
        F.col("a.country").alias("artist_country_2"),
        F.col("a.genre").alias("artist_genre_2"),
    )
)

result = (
    joined_artist
    .select(
        "id",
        F.col("prev3_bpm").alias("prev3_bpm_v"),
        F.col("is_explicit_flag").alias("m_is_explicit_flag"),
    )
    .unpivot(
        ids=["id"],
        values=["prev3_bpm_v", "m_is_explicit_flag"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
