from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_release_date_plu = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date_plus_7d"))
w_all_by_release_date_plus_7d = Window.orderBy("release_date_plus_7d", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy("release_date")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy("release_date", "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last())

cleaned_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

metrics = (
    cleaned_tracks
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_explicit_release_date_plu).alias("rows_in_is_explicit_release_date_plus_7d_month"),
        F.ntile(10).over(w_all_by_release_date_plus_7d).alias("release_date_plus_7d_decile"),
        F.last("duration_sec").over(w_is_explicit_by_duration_sec).alias("last_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.dense_rank().over(w_artist_id_by_release_date_ti).alias("release_date_dense_rank"),
        F.max("bpm").over(w_is_explicit_release_date_plu).alias("is_explicit_release_date_plus_7d_month_max_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit_release_date_plu), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit_release_date_p"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_is_explicit_artist_id_by_rel)).alias("duration_sec_change"),
        F.rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_rank"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_release_date_plu)).alias("distinct_artist_id_in_is_explicit_release_date_p"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "rows_in_is_explicit_release_date_plus_7d_month",
        "last_duration_sec",
        "bpm_share_of_artist_id",
        "release_date_dense_rank",
        "is_explicit_release_date_plus_7d_month_max_bpm",
        "duration_sec_share_of_is_explicit_release_date_p",
        "duration_sec_change",
        "duration_sec_rank",
        "distinct_artist_id_in_is_explicit_release_date_p",
        F.trunc(F.col("release_date_plus_7d"), "MM").alias("release_date_plus_7d_month_start"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
    )
    .orderBy(F.col("bpm_share_of_artist_id").desc_nulls_first())
)
