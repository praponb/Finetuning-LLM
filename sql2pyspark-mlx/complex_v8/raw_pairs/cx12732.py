from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_is_explicit_by_bpm_full = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_artist_id = Window.partitionBy("artist_id")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec") <= 352)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

joined = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("P%"))
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

sequenced = (
    joined
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "artist_is_verified",
        F.lag("bpm").over(w_is_explicit_by_bpm).alias("prev_bpm"),
        F.nth_value("bpm", 2).over(w_is_explicit_by_bpm_full).alias("second_bpm"),
        F.rank().over(w_is_explicit_by_bpm_ties).alias("bpm_rank"),
        F.row_number().over(w_is_explicit_by_bpm).alias("bpm_seq"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "prev_bpm",
        "second_bpm",
        "bpm_rank",
        "bpm_seq",
    )
)
