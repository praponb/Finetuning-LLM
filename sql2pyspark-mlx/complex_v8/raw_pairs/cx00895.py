from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")

filtered_tracks = (
    df_tracks
    .filter(F.col("release_date") < F.lit("2025-08-07").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
    )
)

with_windows = (
    filtered_tracks
    .select(
        "*",
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.first("duration_sec").over(w_artist_id_by_bpm_full).alias("first_duration_sec"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
    )
)

with_details = (
    with_windows.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.debut_year").between(1991, 2006)), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.artist_id_min_duration_sec",
        "s.duration_sec_vs_artist_id_avg",
        "s.first_duration_sec",
        "s.duration_sec_percent_rank",
        F.col("a.genre").alias("artist_genre"),
    )
)

annotated = (
    with_details
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "duration_sec_vs_artist_id_avg",
        "first_duration_sec",
        "duration_sec_percent_rank",
        "artist_genre",
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
)

result = (
    annotated
    .filter(F.col("artist_id") >= 9)
    .select(
        "id",
        "duration_sec_vs_artist_id_avg",
        "first_duration_sec",
        "duration_sec_percent_rank",
        "artist_genre_or_unknown",
        F.greatest(F.col("first_duration_sec"), F.col("duration_sec")).alias("greatest_first_duration_sec_duration_sec"),
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre_or_unknown")), " ", "_")).alias("artist_genre_or_unknown_slug"),
    )
    .orderBy(
        "greatest_first_duration_sec_duration_sec",
        F.col("artist_genre_or_unknown_slug").desc(),
    )
)
