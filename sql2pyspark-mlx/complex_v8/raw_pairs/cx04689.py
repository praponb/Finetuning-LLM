from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_is_explicit = Window.partitionBy("is_explicit")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec").between(165, 352))
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

windowed = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.last("bpm").over(w_is_explicit_by_duration_sec).alias("last_bpm"),
        F.row_number().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_seq"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.dense_rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_dense_rank"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left_semi")
)

derived = (
    without_matches
    .filter(F.col("rows_in_is_explicit") > 10)
    .select(
        "id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_seq",
        "duration_sec_dense_rank",
        F.least(F.col("duration_sec"), F.col("rows_in_is_explicit")).alias("least_duration_sec_rows_in_is_explicit"),
    )
)

result = (
    derived
    .groupingSets(
        [["duration_sec_seq", "duration_sec_dense_rank"], ["duration_sec_seq"], ["duration_sec_dense_rank"]],
        "duration_sec_seq", "duration_sec_dense_rank",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("duration_sec_dense_rank"), 2).alias("avg_duration_sec_dense_rank"),
    )
    .orderBy(
        F.col("duration_sec_seq").asc_nulls_last(),
        F.col("duration_sec_dense_rank").asc_nulls_last(),
    )
)
