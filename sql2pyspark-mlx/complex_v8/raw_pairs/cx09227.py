from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id")
w_artist_id_is_explicit_by_rel_2 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-3, -1)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_is_explicit_by_rel_3 = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("release_date").desc())

recent_tracks = (
    df_tracks
    .filter((F.col("bpm") > 85) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        "moods_text",
        F.nth_value("duration_sec", 2).over(w_is_explicit_artist_id_by_dur).alias("second_duration_sec"),
        F.lead("duration_sec").over(w_artist_id_is_explicit_by_rel).alias("next_duration_sec"),
        F.first("release_date").over(w_artist_id_is_explicit_by_rel_2).alias("first_release_date"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_month), 2).alias("artist_id_release_date_month_avg_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_year), 2).alias("bpm_avg_3_prior"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.row_number().over(w_all_by_duration_sec).alias("duration_sec_seq"),
        F.dense_rank().over(w_artist_id_is_explicit_by_rel_3).alias("release_date_dense_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("release_date_dense_rank") < 7)
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "next_duration_sec",
        "first_release_date",
        "artist_id_release_date_month_avg_bpm",
        "artist_id_sum_duration_sec",
        "duration_sec_seq",
    )
    .orderBy(
        "artist_id_release_date_month_avg_bpm",
        F.col("first_release_date").asc_nulls_last(),
    )
)
