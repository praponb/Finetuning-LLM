from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_bpm_tier = Window.partitionBy("artist_id", "bpm_tier")
w_bpm_tier_by_duration_sec_tie = Window.partitionBy("bpm_tier").orderBy(F.col("duration_sec").desc())
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_id_2 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy("release_date", "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_release_date_moving_3 = Window.orderBy("release_date", "id").rowsBetween(-3, Window.currentRow)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_all_by_release_date_full = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("bpm") >= 142, "HIGH").when(F.col("bpm") >= 104, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

windowed = (
    filtered_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_bpm_tier), 2).alias("bpm_vs_artist_id_bpm_tier_avg"),
        F.round(F.cume_dist().over(w_bpm_tier_by_duration_sec_tie), 3).alias("duration_sec_cume_dist"),
        F.sum("bpm").over(w_release_date_month_artist_id).alias("running_bpm"),
        F.lead("release_date", 3).over(w_release_date_month_artist_id_2).alias("next3_release_date"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.avg("bpm").over(w_all_by_release_date_moving_3), 2).alias("bpm_avg_4"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_release_date)).alias("duration_sec_change"),
        F.rank().over(w_release_date_month_artist_id_2).alias("release_date_rank"),
        F.round(F.avg("duration_sec").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_duration_sec"),
        F.lag("duration_sec", 2, 0).over(w_release_date_month_artist_id_2).alias("prev2_duration_sec"),
        F.last("bpm", ignorenulls=True).over(w_release_date_month_artist_id).alias("last_known_bpm"),
        F.last("release_date").over(w_all_by_release_date_full).alias("last_release_date"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "duration_sec_cume_dist",
        "next3_release_date",
        "rows_in_is_explicit_artist_id",
        "bpm_avg_4",
        "is_explicit_artist_id_avg_duration_sec",
        "prev2_duration_sec",
        "last_known_bpm",
        "last_release_date",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
    .orderBy(
        F.col("rows_in_is_explicit_artist_id").asc(),
        F.col("release_date").desc_nulls_first(),
    )
)
