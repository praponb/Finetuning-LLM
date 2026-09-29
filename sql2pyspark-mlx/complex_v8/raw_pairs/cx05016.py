from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_prior_2 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-2, -1)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 352) & (F.col("bpm") < 147))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
    )
)

with_windows = (
    scoped_tracks
    .select(
        "*",
        F.sum("bpm").over(w_artist_id_by_duration_sec_ru).alias("running_bpm"),
        F.ntile(4).over(w_all_by_duration_sec).alias("duration_sec_quartile"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.lead("duration_sec").over(w_all_by_duration_sec).alias("next_duration_sec"),
        F.max("duration_sec").over(w_all_by_bpm_prior_2).alias("duration_sec_max_2_prior"),
        F.lag("bpm", 1, 0).over(w_artist_id_by_duration_sec).alias("prev_bpm"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.nth_value("duration_sec", 2).over(w_artist_id_by_duration_sec_fu).alias("second_duration_sec"),
        F.nth_value("duration_sec", 2).over(w_all_by_bpm_full).alias("second_duration_sec_2"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_duration_sec)).alias("bpm_change"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.lead("bpm", 3, 0).over(w_all_by_duration_sec).alias("next3_bpm"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_duration_sec"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
    )
)

totals = (
    with_windows
    .groupBy(
        "duration_sec_rank",
        F.when(F.col("bpm") >= 147, "A").when(F.col("bpm") >= 96, "B").otherwise("C").alias("bpm_tier"),
        "duration_sec_quartile",
    )
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 6, F.col("artist_id_max_duration_sec"))).alias("artist_id_max_duration_sec_length_trim_title_6"),
        F.round(F.avg("running_bpm"), 2).alias("avg_running_bpm"),
        F.max("artist_id_min_bpm").alias("max_artist_id_min_bpm"),
        F.sum("duration_sec_max_2_prior").alias("total_duration_sec_max_2_prior"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    totals
    .select(
        "duration_sec_rank",
        "bpm_tier",
        "artist_id_max_duration_sec_length_trim_title_6",
        "avg_running_bpm",
        "max_artist_id_min_bpm",
        "total_duration_sec_max_2_prior",
    )
    .orderBy(F.col("bpm_tier").asc())
)
