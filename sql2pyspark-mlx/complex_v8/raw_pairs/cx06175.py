from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_release_date_month = Window.partitionBy(F.month("release_date"))
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_has_workout = Window.partitionBy("has_workout")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date_month_avg_bpm_per_duration_sec", "id").rowsBetween(-6, Window.currentRow)

recent = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("duration_sec") > 144))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

windowed = (
    recent
    .select(
        "*",
        F.round(F.avg("bpm_per_duration_sec").over(w_artist_id_is_explicit_by_dur), 2).alias("bpm_per_duration_sec_avg_2_prior"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_release_date_month), 2).alias("bpm_vs_release_date_month_avg"),
        F.row_number().over(w_is_explicit_by_duration_sec).alias("duration_sec_seq"),
        F.round(F.avg("bpm_per_duration_sec").over(w_release_date_month), 2).alias("release_date_month_avg_bpm_per_duration_sec"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left_anti")
)

ranked = (
    filtered
    .withColumn("duration_sec_share_of_has_workout", F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_has_workout), F.lit(0)), 4))
    .withColumn("duration_sec_seq_sum_7", F.sum("duration_sec_seq").over(w_is_explicit_by_release_date))
)

filtered_2 = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("S%")), "left_semi")
)

with_activity = (
    filtered_2
    .crossJoin(filtered_2.agg(F.min("duration_sec").alias("overall_min_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_min_duration_sec"))
    .drop("overall_min_duration_sec")
)

combined_details = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.is_verified")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_per_duration_sec",
        "s.bpm_per_duration_sec_avg_2_prior",
        "s.bpm_vs_release_date_month_avg",
        "s.duration_sec_seq",
        "s.release_date_month_avg_bpm_per_duration_sec",
        "s.duration_sec_share_of_has_workout",
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details
    .rollup("is_explicit", "duration_sec_seq", "artist_genre")
    .agg(
        F.sum("bpm").alias("total_bpm"),
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("artist_genre").isin("Indie", "Classical", "Jazz"), 1)).alias("n_artist_genre_in_indie_classi"),
    )
    .select(
        "is_explicit",
        "duration_sec_seq",
        F.coalesce(F.col("artist_genre"), F.lit("ALL")).alias("artist_genre"),
        "total_bpm",
        "n_rows",
        "n_artist_genre_in_indie_classi",
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("duration_sec_seq").asc_nulls_last(),
        F.col("artist_genre").asc_nulls_last(),
    )
)
