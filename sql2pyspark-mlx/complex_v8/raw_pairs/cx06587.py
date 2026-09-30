from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date")).orderBy("bpm_distance", "id").rowsBetween(-2, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")

prepared = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 127).alias("bpm_distance"),
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

tagged = (
    prepared
    .select(
        "*",
        F.abs(F.col("bpm_plus_duration_sec") - 411).alias("bpm_plus_duration_sec_distance"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.is_skipped")) & (F.col("p.ms_played") <= 214343), "left_anti")
)

windowed = (
    screened
    .select(
        "*",
        F.first("bpm_plus_duration_sec").over(w_is_explicit_release_date_mon).alias("first_bpm_plus_duration_sec"),
        F.round(F.avg("bpm_plus_duration_sec").over(w_is_explicit_release_date_mon_2), 2).alias("bpm_plus_duration_sec_avg_3"),
        (F.col("bpm_plus_duration_sec") - F.lag("bpm_plus_duration_sec").over(w_artist_id_by_release_date)).alias("bpm_plus_duration_sec_change"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "bpm_distance",
        "days_since_release_date",
        "first_bpm_plus_duration_sec",
        "bpm_plus_duration_sec_avg_3",
        "bpm_plus_duration_sec_change",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm_plus_duration_sec").cast("string")).alias("title_ref"),
    )
    .orderBy(F.col("bpm_plus_duration_sec_avg_3").desc())
)
