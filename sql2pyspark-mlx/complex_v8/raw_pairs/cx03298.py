from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_moving_2 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")

prepared = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("E%")) & (F.array_contains(F.col("moods"), "workout")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.ms_played") >= 72464) & (F.col("p.source") != "album"), "left_semi")
)

ranked = (
    with_activity
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        "bpm_bucket",
        (F.col("bpm_bucket") - F.lag("bpm_bucket").over(w_all_by_release_date)).alias("bpm_bucket_change"),
        F.max("bpm_bucket").over(w_all_by_release_date_moving_2).alias("bpm_bucket_max_3"),
        F.round(F.avg("bpm_bucket").over(w_release_date_year_artist_id), 2).alias("release_date_year_artist_id_avg_bpm_bucket"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "moods",
        "bpm_bucket_change",
        "bpm_bucket_max_3",
        "release_date_year_artist_id_avg_bpm_bucket",
        F.least(F.col("bpm_bucket_max_3"), F.col("duration_sec")).alias("least_bpm_bucket_max_3_duration_sec"),
    )
    .orderBy(
        F.col("bpm_bucket_max_3").desc(),
        F.col("duration_sec").asc_nulls_last(),
    )
)
