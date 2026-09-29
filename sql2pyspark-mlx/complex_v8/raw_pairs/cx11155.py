from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("moods_count_minus_max").desc(), F.col("id").desc())

valid_tracks = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 4)
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

combined_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods_count",
        "s.bpm_per_duration_sec",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

artist_id_stats = (
    combined_details
    .groupBy("artist_id")
    .agg(F.max("moods_count").alias("max_moods_count"))
)

above_average = (
    combined_details.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.moods_count") == F.col("g.max_moods_count"))
    .select(
        "s.*",
        "g.max_moods_count",
        F.round(F.col("s.moods_count") - F.col("g.max_moods_count"), 2).alias("moods_count_minus_max"),
    )
)

deduped = (
    above_average
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .cube("artist_is_verified", "artist_id")
    .agg(
        F.grouping("artist_is_verified").alias("is_total_artist_is_verified"),
        F.percentile_approx("bpm_per_duration_sec", 0.5).alias("median_bpm_per_duration_sec"),
    )
    .orderBy(
        F.col("artist_is_verified").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
