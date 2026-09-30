from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id = Window.partitionBy("source", "listener_id")
w_source_track_id_by_track_dur = Window.partitionBy("source", "track_id").orderBy("track_duration_sec")
w_track_id_is_skipped_by_track = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("track_release_date").desc())
w_track_id_listener_id_by_play = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc())
w_track_id_is_skipped_by_track_2 = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_track_id_by_track_dur_2 = Window.partitionBy("source", "track_id").orderBy("track_duration_sec", "id")
w_track_id_listener_id_by_play_2 = Window.partitionBy("track_id", "listener_id").orderBy(F.col("played_at").desc(), "id")

base = (
    df_plays
    .filter((F.col("ms_played").between(99376, 239955)) & (F.col("track_id") >= 23))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

combined_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

ranked = (
    combined_details_track
    .select(
        "*",
        F.round(F.col("track_duration_sec") - F.avg("track_duration_sec").over(w_source_listener_id), 2).alias("track_duration_sec_vs_source_listener_id_avg"),
        F.rank().over(w_source_track_id_by_track_dur).alias("track_duration_sec_rank"),
        F.dense_rank().over(w_track_id_is_skipped_by_track).alias("track_release_date_dense_rank"),
        F.min("track_duration_sec").over(w_source_listener_id).alias("source_listener_id_min_track_duration_sec"),
        F.dense_rank().over(w_track_id_listener_id_by_play).alias("played_at_dense_rank"),
        F.sum("track_duration_sec").over(w_track_id_is_skipped_by_track_2).alias("running_track_duration_sec"),
        (F.col("track_duration_sec") - F.lag("track_duration_sec").over(w_source_track_id_by_track_dur_2)).alias("track_duration_sec_change"),
        F.dense_rank().over(w_track_id_listener_id_by_play_2).alias("played_at_dense_rank_3"),
    )
)

enriched = (
    ranked
    .filter(F.col("track_release_date_dense_rank").between(3, 9))
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "track_release_date",
        "track_duration_sec_vs_source_listener_id_avg",
        "track_duration_sec_rank",
        "track_release_date_dense_rank",
        "source_listener_id_min_track_duration_sec",
        "track_duration_sec_change",
        "played_at_dense_rank_3",
        F.year(F.col("track_release_date")).alias("track_release_date_year"),
        F.last_day(F.col("track_release_date")).alias("track_release_date_month_end"),
    )
)

result = (
    enriched
    .filter(F.col("track_duration_sec_rank") >= 3)
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "track_release_date",
        "track_duration_sec_vs_source_listener_id_avg",
        "track_release_date_dense_rank",
        "source_listener_id_min_track_duration_sec",
        "track_duration_sec_change",
        "track_release_date_year",
        "track_release_date_month_end",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy(
        F.col("is_skipped_label").asc_nulls_last(),
        F.col("played_at").asc_nulls_last(),
    )
)
