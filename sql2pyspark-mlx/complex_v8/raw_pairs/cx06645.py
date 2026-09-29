from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_label = Window.partitionBy("is_explicit_label")

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "is_explicit_label",
        F.last("duration_sec").over(w_all_by_release_date_full).alias("last_duration_sec"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.sum("duration_sec").over(w_is_explicit_label).alias("is_explicit_label_sum_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_label), F.lit(0)), 4).alias("bpm_share_of_is_explicit_label"),
    )
)

enriched = ranked.withColumn("release_date_year", F.year(F.col("release_date")))

result = (
    enriched
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "is_explicit_label",
        "duration_sec_share_of_artist_id",
        "duration_sec_vs_artist_id_avg",
        "is_explicit_label_sum_duration_sec",
        "bpm_share_of_is_explicit_label",
        "release_date_year",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
