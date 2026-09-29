from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_genre = Window.partitionBy("artist_id", "artist_genre")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_genre = Window.partitionBy("is_explicit", "artist_genre")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc())
w_artist_genre_by_release_date_2 = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc(), "id")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_parent = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        F.col("a.genre").alias("artist_genre"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_artist_genre), F.lit(0)), 4).alias("bpm_share_of_artist_id_artist_genre"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_artist_genre), 2).alias("duration_sec_vs_artist_id_artist_genre_avg"),
        F.last("bpm", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_bpm"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_artist_genre)).alias("distinct_artist_id_in_is_explicit_artist_genre"),
        F.count(F.lit(1)).over(w_is_explicit_artist_genre).alias("rows_in_is_explicit_artist_genre"),
        F.dense_rank().over(w_artist_genre_by_release_date).alias("release_date_dense_rank"),
        F.lead("duration_sec", 2).over(w_artist_genre_by_release_date_2).alias("next2_duration_sec"),
        F.min("duration_sec").over(w_artist_id_artist_genre).alias("artist_id_artist_genre_min_duration_sec"),
        F.round(F.avg("duration_sec").over(w_is_explicit_artist_genre), 2).alias("is_explicit_artist_genre_avg_duration_sec"),
    )
)

derived = (
    metrics
    .filter(F.col("duration_sec_vs_artist_id_artist_genre_avg") < 168.0)
    .withColumn("duration_sec_per_next2_duration_sec", F.round(F.col("duration_sec") / F.nullif(F.col("next2_duration_sec"), F.lit(0)), 3))
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm_share_of_artist_id_artist_genre",
        "last_known_bpm",
        "distinct_artist_id_in_is_explicit_artist_genre",
        "release_date_dense_rank",
        "next2_duration_sec",
        "duration_sec_per_next2_duration_sec",
        F.when(F.col("next2_duration_sec") >= 352, "A").when(F.col("next2_duration_sec") >= 184, "B").otherwise("C").alias("next2_duration_sec_tier"),
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
    .orderBy(
        F.col("bpm_share_of_artist_id_artist_genre").desc_nulls_first(),
        F.col("last_known_bpm").asc_nulls_last(),
    )
)
