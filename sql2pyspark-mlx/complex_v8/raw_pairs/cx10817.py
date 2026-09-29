from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy(F.col("pct_is_explicit_true").desc(), "is_explicit", "artist_is_verified")
w_is_explicit_artist_is_verifi_2 = Window.partitionBy("is_explicit", "artist_is_verified").orderBy(F.col("pct_is_explicit_true").desc(), "is_explicit", "artist_is_verified").rowsBetween(-6, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("E%")) | (F.col("duration_sec") >= 209))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        F.abs(F.col("duration_sec") - 344).alias("duration_sec_distance"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

with_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre") == "HipHop")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.duration_sec_distance",
        "s.bpm_filled",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

qualified = (
    with_details_artist.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date") >= F.lit("2024-11-06").cast("date")) & (F.col("t.bpm") >= 85), "left_anti")
)

with_parent = (
    qualified.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm_filled",
        "s.artist_genre",
        "s.artist_debut_year",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source").isin("search", "playlist", "radio")) & (F.col("p.listener_id") <= 9), "left_anti")
)

per_group = (
    with_activity
    .groupBy("is_explicit", "artist_is_verified")
    .agg(
        F.round(100 * F.count(F.when(F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_is_explicit_true"),
        F.sum("bpm_filled").alias("total_bpm_filled"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
        F.avg("artist_debut_year").alias("avg_artist_debut_year_check"),
    )
    .filter(F.col("avg_artist_debut_year_check") > 1997)
    .drop("avg_artist_debut_year_check")
)

metrics = (
    per_group
    .select(
        "*",
        F.lead("total_bpm_filled").over(w_is_explicit_artist_is_verifi).alias("next_total_bpm_filled"),
        F.rank().over(w_is_explicit_artist_is_verifi).alias("pct_is_explicit_true_rank"),
        F.round(F.avg("total_bpm_filled").over(w_is_explicit_artist_is_verifi_2), 2).alias("total_bpm_filled_avg_7"),
    )
)

result = (
    metrics
    .groupingSets(
        [["is_explicit", "pct_is_explicit_true_rank", "artist_is_verified"], ["is_explicit"], []],
        "is_explicit", "pct_is_explicit_true_rank", "artist_is_verified",
    )
    .agg(
        F.min("pct_is_explicit_true_rank").alias("min_pct_is_explicit_true_rank"),
        F.percentile_approx("pct_is_explicit_true", 0.5).alias("median_pct_is_explicit_true"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("pct_is_explicit_true_rank").asc_nulls_last(),
        F.col("artist_is_verified").asc_nulls_last(),
    )
)
