from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_is_explicit_by_day = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("days_since_release_date").desc(), "id")
w_all_by_duration_sec_ties_2 = Window.orderBy("duration_sec")
w_all_by_duration_sec_moving_1 = Window.orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)

base_tracks = (
    df_tracks
    .filter(F.col("bpm").between(76, 170))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
)

sequenced = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "title_ref",
        F.last("days_since_release_date", ignorenulls=True).over(w_is_explicit_artist_id_by_dur).alias("last_known_days_since_release_date"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_duration_sec"),
        F.round(F.avg("days_since_release_date").over(w_is_explicit), 2).alias("is_explicit_avg_days_since_release_date"),
        F.sum("days_since_release_date").over(w_artist_id).alias("artist_id_sum_days_since_release_date"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.round(F.avg("bpm").over(w_is_explicit), 2).alias("is_explicit_avg_bpm"),
        (F.col("days_since_release_date") - F.lag("days_since_release_date").over(w_all_by_duration_sec)).alias("days_since_release_date_change"),
        F.ntile(10).over(w_artist_id_is_explicit_by_day).alias("days_since_release_date_decile"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties_2), 3).alias("duration_sec_cume_dist"),
        F.sum("bpm").over(w_all_by_duration_sec_moving_1).alias("bpm_sum_2"),
        F.round(F.avg("duration_sec").over(w_is_explicit_artist_id_by_dur_2), 2).alias("duration_sec_avg_7"),
        F.min("bpm").over(w_is_explicit).alias("is_explicit_min_bpm"),
        F.lag("duration_sec", 1, 0).over(w_all_by_duration_sec).alias("prev_duration_sec"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "title_ref",
        "last_known_days_since_release_date",
        "is_explicit_avg_days_since_release_date",
        "artist_id_sum_days_since_release_date",
        "bpm_vs_is_explicit_avg",
        "duration_sec_percent_rank",
        "is_explicit_avg_bpm",
        "days_since_release_date_change",
        "days_since_release_date_decile",
        "duration_sec_cume_dist",
        "duration_sec_avg_7",
        "is_explicit_min_bpm",
        "prev_duration_sec",
    )
    .orderBy(F.col("artist_id").desc(), F.col("id").desc())
    .limit(10)
)
