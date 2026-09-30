from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")

base = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

tagged = (
    base
    .withColumn("bpm_per_duration_sec_minus_bpm", F.col("bpm_per_duration_sec") - F.col("bpm"))
)

with_windows = (
    tagged
    .select(
        "id",
        "artist_id",
        "bpm",
        "release_date_year",
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.sum("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_bpm"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "bpm",
        "release_date_year",
        "rows_in_is_explicit_artist_id",
        "is_explicit_artist_id_sum_bpm",
    )
    .orderBy(F.col("rows_in_is_explicit_artist_id").asc_nulls_last())
)
