from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_track_is_explicit = Window.partitionBy("is_skipped", "track_is_explicit").orderBy("played_at_year")
w_is_skipped_track_is_explicit_2 = Window.partitionBy("is_skipped", "track_is_explicit").orderBy("played_at_year", "id")
w_track_id_by_played_at_year_f = Window.partitionBy("track_id").orderBy(F.col("played_at_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_played_at_year = Window.partitionBy("track_id").orderBy("played_at_year", "id")
w_is_skipped_track_is_explicit_3 = Window.partitionBy("is_skipped", "track_is_explicit").orderBy("played_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit_by_last_kn = Window.partitionBy("track_is_explicit").orderBy("last_known_played_at_year", "id")
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_listener_id_played_at_year_t = Window.partitionBy("listener_id", F.to_date("played_at_year")).orderBy("last_played_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_played_at_year_t_2 = Window.partitionBy("listener_id", F.to_date("played_at_year")).orderBy("last_played_at_year", "id")

scoped_plays = (
    df_plays
    .filter((~F.col("source").isin("search", "radio")) & (F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("tv", "phone"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
    )
)

with_parent_track = (
    scoped_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.array_contains(F.col("t.moods"), "chill")), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.device_group",
        "s.played_at_year",
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

with_windows = (
    with_parent_track
    .select(
        "id",
        "listener_id",
        "source",
        "device_group",
        "played_at_year",
        "track_is_explicit",
        F.round(F.percent_rank().over(w_is_skipped_track_is_explicit), 3).alias("played_at_year_percent_rank"),
        F.round(F.cume_dist().over(w_is_skipped_track_is_explicit), 3).alias("played_at_year_cume_dist"),
        F.dense_rank().over(w_is_skipped_track_is_explicit_2).alias("played_at_year_dense_rank"),
        F.last("played_at_year").over(w_track_id_by_played_at_year_f).alias("last_played_at_year"),
        F.rank().over(w_track_id_by_played_at_year).alias("played_at_year_rank"),
        F.lead("played_at_year", 3).over(w_track_id_by_played_at_year).alias("next3_played_at_year"),
        F.last("played_at_year", ignorenulls=True).over(w_is_skipped_track_is_explicit_3).alias("last_known_played_at_year"),
        F.lead("played_at_year").over(w_track_id_by_played_at_year).alias("next_played_at_year"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "listener_id",
        "source",
        "device_group",
        "played_at_year",
        "played_at_year_cume_dist",
        "played_at_year_rank",
        "next_played_at_year",
        F.ntile(4).over(w_track_is_explicit_by_last_kn).alias("last_known_played_at_year_quartile"),
        F.round(F.col("played_at_year_rank") - F.avg("played_at_year_rank").over(w_track_is_explicit), 2).alias("played_at_year_rank_vs_track_is_explicit_avg"),
        F.first("played_at_year_rank", ignorenulls=True).over(w_listener_id_played_at_year_t).alias("first_known_played_at_year_rank"),
        F.lag("played_at_year_percent_rank").over(w_listener_id_played_at_year_t_2).alias("prev_played_at_year_percent_rank"),
        F.round(F.col("played_at_year_percent_rank") - F.avg("played_at_year_percent_rank").over(w_track_is_explicit), 2).alias("played_at_year_percent_rank_vs_track_is_explicit"),
        (F.col("played_at_year_rank") - F.lag("played_at_year_rank").over(w_listener_id_played_at_year_t_2)).alias("played_at_year_rank_change"),
        F.min("played_at_year_percent_rank").over(w_track_is_explicit).alias("track_is_explicit_min_played_at_year_percent_ran"),
        F.lag("last_played_at_year").over(w_listener_id_played_at_year_t_2).alias("prev_last_played_at_year"),
    )
)

result = (
    ranked
    .filter(F.col("prev_played_at_year_percent_rank") <= 0.8)
    .select(
        "id",
        "listener_id",
        "source",
        "device_group",
        "played_at_year_cume_dist",
        "played_at_year_rank",
        "next_played_at_year",
        "last_known_played_at_year_quartile",
        "played_at_year_rank_vs_track_is_explicit_avg",
        "first_known_played_at_year_rank",
        "prev_played_at_year_percent_rank",
        "played_at_year_percent_rank_vs_track_is_explicit",
        "played_at_year_rank_change",
        "track_is_explicit_min_played_at_year_percent_ran",
        "prev_last_played_at_year",
    )
    .orderBy(F.col("listener_id").asc(), "id")
)
