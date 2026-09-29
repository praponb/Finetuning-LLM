from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy("release_date", "id")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc_nulls_first())
w_is_explicit_artist_id_by_rel_3 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tracks
    .filter(F.col("artist_id") >= 9)
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
)

with_windows = (
    recent
    .select(
        "*",
        F.rank().over(w_is_explicit_artist_id_by_rel).alias("release_date_rank"),
        F.lag("release_date", 3).over(w_release_date_month_artist_id).alias("prev3_release_date"),
        F.dense_rank().over(w_is_explicit_artist_id_by_rel_2).alias("release_date_dense_rank"),
        F.last("release_date", ignorenulls=True).over(w_is_explicit_artist_id_by_rel_3).alias("last_known_release_date"),
        F.first("release_date").over(w_is_explicit_by_release_date).alias("first_release_date"),
    )
)

with_parent = (
    with_windows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
        F.count("*").alias("track_count"),
    )
)

summarized_rows = (
    with_parent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        "t.avg_track_bpm",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["track_count", "artist_country"], ["track_count"]],
        "track_count", "artist_country",
    )
    .agg(
        F.grouping("track_count").alias("is_total_track_count"),
        F.max("track_count").alias("max_track_count"),
    )
    .select(
        "track_count",
        F.coalesce(F.col("artist_country"), F.lit("ALL")).alias("artist_country"),
        "is_total_track_count",
        "max_track_count",
    )
    .orderBy(
        F.col("track_count").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
    )
)
