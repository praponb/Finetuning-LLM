from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_artist_id_is_explicit_by_dur_3 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_second_durati = Window.partitionBy("is_explicit").orderBy("second_duration_sec", "id")

cleaned = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-03-29").cast("date"))) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
    )
)

sequenced = (
    cleaned
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_is_explicit_by_dur).alias("first_known_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_artist_id_is_explicit_by_dur_2).alias("second_duration_sec"),
        F.max("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_duration_sec"),
        F.lag("bpm", 2).over(w_artist_id_is_explicit_by_dur_3).alias("prev2_bpm"),
    )
)

with_windows = (
    sequenced
    .select(
        "id",
        "second_duration_sec",
        "prev2_bpm",
        F.lead("first_known_duration_sec", 1, 0).over(w_is_explicit_by_second_durati).alias("next_first_known_duration_sec"),
        F.dense_rank().over(w_is_explicit_by_second_durati).alias("second_duration_sec_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "second_duration_sec",
        "prev2_bpm",
        "next_first_known_duration_sec",
        "second_duration_sec_dense_rank",
    )
)
