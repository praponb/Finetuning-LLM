from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", F.year("release_date")).orderBy(F.col("release_date").desc_nulls_first(), "id")
w_is_explicit_release_date_yea_2 = Window.partitionBy("is_explicit", F.year("release_date")).orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("duration_sec").desc())
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_duration_sec_cu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) | (F.col("duration_sec") <= 353))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

metrics = (
    prepared_tracks
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.lead("duration_sec").over(w_is_explicit_release_date_yea).alias("next_duration_sec"),
        F.nth_value("release_date", 2).over(w_is_explicit_release_date_yea_2).alias("second_release_date"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.row_number().over(w_is_explicit_release_date_yea).alias("rn"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.round(F.cume_dist().over(w_is_explicit_release_date_yea), 3).alias("release_date_cume_dist"),
        F.lead("duration_sec").over(w_is_explicit_artist_id_by_dur).alias("next_duration_sec_2"),
        F.round(F.cume_dist().over(w_is_explicit_artist_id_by_dur_2), 3).alias("duration_sec_cume_dist"),
        F.ntile(4).over(w_artist_id_by_release_date).alias("release_date_quartile"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_release_date_mo), 2).alias("duration_sec_avg_2"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "title_prefix",
        "rows_in_artist_id",
        "second_release_date",
        "rn",
        "duration_sec_vs_artist_id_avg",
        "artist_id_max_duration_sec",
        "release_date_cume_dist",
        "next_duration_sec_2",
        "duration_sec_cume_dist",
        "release_date_quartile",
        "duration_sec_avg_2",
        F.round(F.avg("rows_in_artist_id").over(w_artist_id), 2).alias("artist_id_avg_rows_in_artist_id"),
        F.round(F.col("release_date_quartile") / F.nullif(F.sum("release_date_quartile").over(w_artist_id), F.lit(0)), 4).alias("release_date_quartile_share_of_artist_id"),
        F.count(F.lit(1)).over(w_artist_id_by_duration_sec_cu).alias("running_count"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "release_date",
        "title_prefix",
        "rows_in_artist_id",
        "release_date_cume_dist",
        "next_duration_sec_2",
        "duration_sec_cume_dist",
        "release_date_quartile",
        "duration_sec_avg_2",
        "release_date_quartile_share_of_artist_id",
        "running_count",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
    .orderBy(F.col("duration_sec_cume_dist").desc(), "id")
    .limit(10)
)
