from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_bpm = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id")

filtered = (
    df_tracks
    .filter(~F.array_contains(F.col("moods"), "chill"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.least_duration_sec_bpm",
        "s.title_prefix",
        "a.artist_name",
    )
)

with_windows = (
    joined
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "least_duration_sec_bpm",
        "title_prefix",
        "artist_name",
        F.dense_rank().over(w_release_date_month_by_bpm).alias("bpm_dense_rank"),
        F.ntile(4).over(w_release_date_month_by_bpm).alias("bpm_quartile"),
    )
)

result = (
    with_windows
    .filter(F.lower(F.col("title")).like("%ar%"))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "least_duration_sec_bpm",
        "title_prefix",
        "artist_name",
        "bpm_dense_rank",
        "bpm_quartile",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.coalesce(F.col("least_duration_sec_bpm"), F.lit(0)).alias("least_duration_sec_bpm_filled"),
    )
    .orderBy(F.col("artist_id").asc(), "bpm_bucket")
)
