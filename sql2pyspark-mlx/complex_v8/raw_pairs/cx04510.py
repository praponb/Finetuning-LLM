from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.col("duration_sec") > 213)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

exploded = (
    base
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "duration_sec_plus_bpm",
        "release_date_month_start",
        "moods_count",
        F.explode_outer("moods").alias("mood"),
    )
)

result = (
    exploded
    .groupingSets(
        [["artist_id", "mood", "moods_count"], ["artist_id"]],
        "artist_id", "mood", "moods_count",
    )
    .agg(
        F.grouping("artist_id").alias("is_total_artist_id"),
        F.round(F.sum("moods_count") / F.nullif(F.sum("duration_sec_plus_bpm"), F.lit(0)), 3).alias("moods_count_to_duration_sec_plus_bpm_ratio"),
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("mood").asc_nulls_last(),
        F.col("moods_count").asc_nulls_last(),
    )
)
