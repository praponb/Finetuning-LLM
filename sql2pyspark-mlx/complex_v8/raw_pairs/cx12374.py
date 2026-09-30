from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

per_group = (
    recent
    .groupBy("artist_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.sum("duration_sec_minus_bpm").alias("total_duration_sec_minus_bpm"),
        F.round(F.avg("duration_sec_minus_bpm"), 2).alias("avg_duration_sec_minus_bpm"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.avg("duration_sec").alias("avg_duration_sec_check"),
    )
    .filter(F.col("avg_duration_sec_check") > 142)
    .drop("avg_duration_sec_check")
)

joined = (
    per_group.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

enriched_artist = (
    joined.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified_2"),
    )
)

combined_details = (
    enriched_artist.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.artist_id",
        "s.total_duration_sec",
        "s.total_duration_sec_minus_bpm",
        "s.avg_duration_sec_minus_bpm",
        "s.distinct_artist_ids",
        "s.title_list",
        "s.artist_name",
        "s.artist_country",
        "s.artist_is_verified_2",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified_3"),
    )
)

result = (
    combined_details
    .select(
        "artist_id",
        "total_duration_sec",
        "avg_duration_sec_minus_bpm",
        "distinct_artist_ids",
        "title_list",
        "artist_is_verified_2",
        F.round(F.col("total_duration_sec") / F.nullif(F.col("distinct_artist_ids"), F.lit(0)), 3).alias("total_duration_sec_per_distinct_artist_ids"),
        F.abs(F.col("distinct_artist_ids") - 7).alias("distinct_artist_ids_distance"),
    )
    .orderBy(
        F.col("distinct_artist_ids_distance").desc(),
        F.col("total_duration_sec").asc_nulls_last(),
    )
)
