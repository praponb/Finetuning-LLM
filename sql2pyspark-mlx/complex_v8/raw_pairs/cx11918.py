from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_dur_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_is_explicit_by_dur_3 = Window.partitionBy("artist_id", "is_explicit").orderBy("duration_sec")
w_latest = Window.partitionBy("artist_id").orderBy("first_duration_sec", "id")

base = (
    df_tracks
    .filter(F.col("bpm") <= 142)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
    )
)

sequenced = (
    base
    .select(
        "*",
        F.first("duration_sec").over(w_artist_id_is_explicit_by_dur).alias("first_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_is_explicit_by_dur_2)).alias("duration_sec_change"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.rank().over(w_artist_id_is_explicit_by_dur_3).alias("duration_sec_rank"),
    )
)

latest = (
    sequenced
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    latest
    .filter(F.col("duration_sec").between(183, 352))
    .select(
        "*",
        F.least(F.col("first_duration_sec"), F.col("duration_sec_rank")).alias("least_first_duration_sec_duration_sec_rank"),
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 127) & (F.col("t.release_date") < F.lit("2025-03-08").cast("date")), "left_semi")
    .filter(F.col("is_explicit"))
)

result = (
    qualified
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
        "first_duration_sec",
        "duration_sec_change",
        "duration_sec_rank",
        "least_first_duration_sec_duration_sec_rank",
        "has_workout",
        F.coalesce(F.col("duration_sec_change"), F.lit(0)).alias("duration_sec_change_filled"),
    )
    .orderBy(F.col("duration_sec_change").desc(), "id")
)
