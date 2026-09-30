from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("genre", "country").orderBy(F.col("debut_year_filled_2").desc(), F.col("id").desc())
w_all_by_min_track_bpm_moving = Window.orderBy(F.col("min_track_bpm").desc(), "artist_is_verified", "genre", "is_verified").rowsBetween(-1, Window.currentRow)
w_genre_is_verified = Window.partitionBy("genre", "is_verified")

filtered_artists = (
    df_artists
    .filter(F.col("is_verified"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled_2"),
    )
)

unique_rows = (
    filtered_artists
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

detail_rows = (
    unique_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.genre",
        "s.country",
        "s.is_verified",
        "s.debut_year_filled",
        F.col("t.id").alias("track_id"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.moods").alias("track_moods"),
    )
)

joined_track = (
    detail_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.genre",
        "s.is_verified",
        "s.debut_year_filled",
        "s.track_id",
        "s.track_is_explicit",
        "s.track_moods",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit_2"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

rollup_base = (
    joined_track
    .filter(~F.col("track_is_explicit"))
    .groupBy("artist_is_verified", "genre", "is_verified")
    .agg(
        F.min("track_bpm").alias("min_track_bpm"),
        F.max("debut_year_filled").alias("max_debut_year_filled"),
        F.datediff(F.max("track_release_date"), F.min("track_release_date")).alias("track_release_date_span_days"),
        F.sum("track_bpm").alias("sum_track_bpm_check"),
    )
    .filter(F.col("sum_track_bpm_check") > 75)
    .drop("sum_track_bpm_check")
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("min_track_bpm").alias("overall_max_min_track_bpm")))
    .filter(F.col("min_track_bpm") < F.col("overall_max_min_track_bpm"))
    .drop("overall_max_min_track_bpm")
)

metrics = (
    with_activity
    .select(
        "*",
        F.sum("min_track_bpm").over(w_all_by_min_track_bpm_moving).alias("min_track_bpm_sum_2"),
        F.round(F.col("max_debut_year_filled") - F.avg("max_debut_year_filled").over(w_genre_is_verified), 2).alias("max_debut_year_filled_vs_genre_is_verified_avg"),
    )
)

result = (
    metrics
    .select(
        "artist_is_verified",
        "is_verified",
        "min_track_bpm",
        "max_debut_year_filled",
        "track_release_date_span_days",
        "min_track_bpm_sum_2",
        "max_debut_year_filled_vs_genre_is_verified_avg",
        (F.floor(F.col("min_track_bpm") / 25) * 25).alias("min_track_bpm_bucket"),
    )
)
