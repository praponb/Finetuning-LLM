from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_all_by_bpm_ties = Window.orderBy("bpm")
w_is_explicit_artist_id_by_is = Window.partitionBy("is_explicit", "artist_id").orderBy("is_explicit_sum_bpm", "id")
w_is_explicit_artist_id_by_is_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("is_explicit_sum_bpm").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")

prepared_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 365)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

with_windows = (
    prepared_tracks
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_is_explicit_by_duration_sec), 2).alias("duration_sec_avg_3_prior"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.round(F.percent_rank().over(w_is_explicit_by_duration_sec_2), 3).alias("duration_sec_percent_rank"),
        F.min("duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_min_duration_sec"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_is_explicit_by_bpm)).alias("bpm_change"),
        F.lead("duration_sec", 1, 0).over(w_artist_id_is_explicit_by_bpm).alias("next_duration_sec"),
        F.sum("bpm").over(w_is_explicit).alias("is_explicit_sum_bpm"),
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
    )
)

leaders = (
    with_windows
    .filter((F.col("bpm_dense_rank") <= 3) & (F.col("bpm") >= 94))
    .drop("bpm_dense_rank")
)

with_trends = (
    leaders
    .select(
        "*",
        F.lead("bpm", 2, 0).over(w_is_explicit_artist_id_by_is).alias("next2_bpm"),
        F.ntile(3).over(w_is_explicit_artist_id_by_is_2).alias("is_explicit_sum_bpm_tercile"),
        F.row_number().over(w_is_explicit_artist_id_by_is).alias("rn"),
        F.min("duration_sec").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_duration_sec"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_is_explicit_artist_id_by_is)).alias("duration_sec_change"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "bpm",
        "title_slug",
        "bpm_share_of_is_explicit",
        "duration_sec_percent_rank",
        "artist_id_is_explicit_min_duration_sec",
        "bpm_change",
        "next_duration_sec",
        "is_explicit_sum_bpm",
        "next2_bpm",
        "is_explicit_sum_bpm_tercile",
        "rn",
        "rows_in_is_explicit_artist_id",
        F.upper(F.trim(F.col("title_slug"))).alias("title_slug_clean"),
        F.initcap(F.trim(F.col("title_slug"))).alias("title_slug_clean_2"),
    )
    .orderBy(F.col("id").desc(), F.col("is_explicit_sum_bpm").desc_nulls_first())
    .limit(20)
)
