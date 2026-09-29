from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec_per_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))

prepared = (
    df_tracks
    .filter(F.col("artist_id") >= 12)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec").between(165, 402)), "left_semi")
)

with_parent = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.debut_year").alias("artist_debut_year"))
)

with_windows = (
    with_parent
    .select(
        "*",
        F.nth_value("release_date", 2).over(w_artist_id_is_explicit_by_dur).alias("second_release_date"),
        F.round(F.avg("duration_sec_per_bpm").over(w_artist_id_release_date_month), 2).alias("artist_id_release_date_month_avg_duration_sec_pe"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        "duration_sec_per_bpm",
        "artist_debut_year",
        "artist_id_release_date_month_avg_duration_sec_pe",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.round(F.col("duration_sec_per_bpm") * 1.5, 2).alias("duration_sec_per_bpm_adj"),
    )
    .orderBy(
        F.col("id").desc_nulls_first(),
        F.col("duration_sec_per_bpm_adj").asc_nulls_last(),
    )
)
