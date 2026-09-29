from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc())
w_release_date_seq = Window.partitionBy("release_date_seq")
w_all_by_first_known_release_d = Window.orderBy("first_known_release_date", "id")
w_artist_id_by_duration_sec_sh = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_share_of_artist_id_is_explicit").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-11-14").cast("date")) & (F.col("bpm") >= 76))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

sequenced = (
    base
    .select(
        "*",
        F.last("release_date", ignorenulls=True).over(w_is_explicit_by_release_date).alias("last_known_release_date"),
        F.dense_rank().over(w_is_explicit_by_release_date_2).alias("release_date_dense_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        F.lag("duration_sec", 2).over(w_artist_id_is_explicit_by_rel).alias("prev2_duration_sec"),
        F.row_number().over(w_is_explicit_by_release_date_2).alias("release_date_seq"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_dur), 3).alias("duration_sec_percent_rank"),
        F.round(F.cume_dist().over(w_is_explicit_by_release_date_2), 3).alias("release_date_cume_dist"),
        F.first("release_date", ignorenulls=True).over(w_is_explicit_by_release_date).alias("first_known_release_date"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_release_date_seq)).alias("distinct_artist_id_in_release_date_seq"),
        F.dense_rank().over(w_all_by_first_known_release_d).alias("first_known_release_date_dense_rank"),
        F.nth_value("duration_sec_percent_rank", 2).over(w_artist_id_by_duration_sec_sh).alias("second_duration_sec_percent_rank"),
        F.round(F.col("release_date_dense_rank") - F.avg("release_date_dense_rank").over(w_release_date_seq), 2).alias("release_date_dense_rank_vs_release_date_seq_avg"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "last_known_release_date",
        "release_date_dense_rank",
        "prev2_duration_sec",
        "release_date_seq",
        "release_date_cume_dist",
        "first_known_release_date",
        "duration_sec_share_of_artist_id_is_explicit",
        "distinct_artist_id_in_release_date_seq",
        "first_known_release_date_dense_rank",
    )
    .orderBy(
        F.col("first_known_release_date").desc(),
        F.col("first_known_release_date_dense_rank").desc(),
    )
)
