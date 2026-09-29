from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_artist_ids_filled = Window.partitionBy("distinct_artist_ids_filled")
w_artist_id_by_bpm_to_duration = Window.partitionBy("artist_id").orderBy(F.col("bpm_to_duration_sec_ratio").desc_nulls_first(), "release_date_ym", "artist_id").rowsBetween(-2, -1)

prepared_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.col("duration_sec") > 239))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

summary = (
    prepared_tracks
    .groupBy(
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "artist_id",
    )
    .agg(
        F.sum(F.when(F.upper(F.trim(F.col("title"))).like("G%"), 1).otherwise(0)).alias("n_upper_trim_title_like_g"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.round(F.sum("bpm") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("bpm_to_duration_sec_ratio"),
        F.array_sort(F.collect_set("title")).alias("title_list"),
    )
)

annotated = (
    summary
    .filter(F.col("distinct_artist_ids").between(1, 9))
    .select(
        "release_date_ym",
        "artist_id",
        "n_upper_trim_title_like_g",
        "bpm_to_duration_sec_ratio",
        "title_list",
        F.upper(F.trim(F.col("release_date_ym"))).alias("release_date_ym_clean"),
        F.coalesce(F.col("distinct_artist_ids"), F.lit(0)).alias("distinct_artist_ids_filled"),
    )
)

with_parent_artist = (
    annotated.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
    )
)

metrics = (
    with_parent_artist
    .select(
        "release_date_ym",
        "artist_id",
        "n_upper_trim_title_like_g",
        "bpm_to_duration_sec_ratio",
        "title_list",
        "release_date_ym_clean",
        "artist_is_verified",
        "artist_debut_year",
        F.round(F.col("bpm_to_duration_sec_ratio") / F.nullif(F.sum("bpm_to_duration_sec_ratio").over(w_distinct_artist_ids_filled), F.lit(0)), 4).alias("bpm_to_duration_sec_ratio_share_of_distinct_arti"),
        F.max("distinct_artist_ids_filled").over(w_artist_id_by_bpm_to_duration).alias("distinct_artist_ids_filled_max_2_prior"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.min("release_date").alias("first_track_release_date"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

profiled = (
    metrics.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.first_track_release_date",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

result = (
    profiled
    .select(
        "release_date_ym",
        "n_upper_trim_title_like_g",
        "bpm_to_duration_sec_ratio",
        "title_list",
        "release_date_ym_clean",
        "artist_debut_year",
        "bpm_to_duration_sec_ratio_share_of_distinct_arti",
        "distinct_artist_ids_filled_max_2_prior",
        "track_count",
        "first_track_release_date",
        "track_is_explicit_count",
    )
    .orderBy(F.col("release_date_ym_clean").desc())
)
