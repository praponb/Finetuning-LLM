from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_least_duration = Window.partitionBy("artist_id").orderBy("least_duration_sec_bpm", "id")
w_artist_id = Window.partitionBy("artist_id")

cleaned = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) | (F.col("artist_id") <= 7))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        "title_clean",
        F.row_number().over(w_artist_id_by_least_duration).alias("rn"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_by_least_duration)).alias("duration_sec_change"),
        F.min("least_duration_sec_bpm").over(w_artist_id).alias("artist_id_min_least_duration_sec_bpm"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "title_clean",
        "artist_id_min_least_duration_sec_bpm",
        (F.col("duration_sec_change") - F.col("bpm")).alias("duration_sec_change_minus_bpm"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

result = (
    scored
    .filter(F.col("title_clean").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "title_clean",
        "artist_id_min_least_duration_sec_bpm",
        "bpm_filled",
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm_filled"), F.lit(0)), 3).alias("duration_sec_per_bpm_filled"),
    )
    .orderBy(F.col("artist_id_min_least_duration_sec_bpm").asc_nulls_last())
)
