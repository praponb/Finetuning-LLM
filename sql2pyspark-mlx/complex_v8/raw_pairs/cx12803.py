from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_all_by_duration_sec_prior_6 = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.col("duration_sec") > 154))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
    )
)

joined = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "a.artist_name",
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
        F.rank().over(w_all_by_bpm).alias("bpm_rank"),
        F.last("release_date").over(w_all_by_bpm_full).alias("last_release_date"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.lead("bpm", 2).over(w_all_by_bpm).alias("next2_bpm"),
        F.lead("bpm").over(w_all_by_duration_sec).alias("next_bpm"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.sum("bpm").over(w_all_by_duration_sec_prior_6).alias("bpm_sum_6_prior"),
        F.ntile(10).over(w_all_by_bpm).alias("bpm_decile"),
        F.round(F.avg("duration_sec").over(w_artist_id), 2).alias("artist_id_avg_duration_sec"),
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.first("bpm", ignorenulls=True).over(w_all_by_release_date_running).alias("first_known_bpm"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        "second_duration_sec",
        "bpm_rank",
        "last_release_date",
        "last_known_duration_sec",
        "rows_in_artist_id",
        "next2_bpm",
        "next_bpm",
        "artist_id_avg_bpm",
        "bpm_sum_6_prior",
        "bpm_decile",
        "artist_id_avg_duration_sec",
        "artist_id_sum_duration_sec",
    )
    .orderBy(F.col("last_known_duration_sec").desc())
)
