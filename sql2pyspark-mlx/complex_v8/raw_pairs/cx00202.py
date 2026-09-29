from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_listener_id_track_id_by_list = Window.partitionBy("listener_id", "track_id").orderBy("listener_birth_year", "id")
w_is_skipped_source_by_played = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)
w_is_skipped_source_by_played_2 = Window.partitionBy("is_skipped", "source").orderBy(F.col("played_at").desc(), "id")
w_listener_id_track_id_by_play_2 = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id")
w_device_played_at_month = Window.partitionBy("device", F.date_trunc("month", "played_at"))
w_ms_played_dense_rank_played = Window.partitionBy("ms_played_dense_rank", F.date_trunc("month", "played_at")).orderBy("prev_listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_dense_rank_played_2 = Window.partitionBy("ms_played_dense_rank", F.date_trunc("month", "played_at")).orderBy("prev_listener_birth_year", "id")
w_played_at_quartile_ms_played = Window.partitionBy("played_at_quartile", "ms_played_dense_rank")

cleaned_plays = (
    df_plays
    .filter(F.col("source").isin("playlist", "album"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

combined_details = (
    cleaned_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "listener_interests",
        "listener_birth_year",
        "listener_username",
        F.lag("listener_birth_year").over(w_is_skipped_by_ms_played).alias("prev_listener_birth_year"),
        F.dense_rank().over(w_is_skipped_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.cume_dist().over(w_listener_id_track_id_by_list), 3).alias("listener_birth_year_cume_dist"),
        F.last("ms_played", ignorenulls=True).over(w_is_skipped_source_by_played).alias("last_known_ms_played"),
        F.sum("listener_birth_year").over(w_listener_id_track_id_by_play).alias("listener_birth_year_sum_7"),
        F.lag("listener_birth_year", 1, 0).over(w_is_skipped_source_by_played_2).alias("prev_listener_birth_year_2"),
        F.ntile(4).over(w_listener_id_track_id_by_play_2).alias("played_at_quartile"),
        F.round(F.avg("listener_birth_year").over(w_device_played_at_month), 2).alias("device_played_at_month_avg_listener_birth_year"),
    )
)

metrics = (
    ranked
    .select(
        "*",
        F.last("listener_birth_year_sum_7", ignorenulls=True).over(w_ms_played_dense_rank_played).alias("last_known_listener_birth_year_sum_7"),
        F.lag("played_at", 2).over(w_ms_played_dense_rank_played_2).alias("prev2_played_at"),
        F.round(F.avg("played_at_quartile").over(w_played_at_quartile_ms_played), 2).alias("played_at_quartile_ms_played_dense_rank_avg_play"),
        F.round(F.col("prev_listener_birth_year_2") - F.avg("prev_listener_birth_year_2").over(w_played_at_quartile_ms_played), 2).alias("prev_listener_birth_year_2_vs_played_at_quartile"),
        F.max("last_known_ms_played").over(w_played_at_quartile_ms_played).alias("played_at_quartile_ms_played_dense_rank_max_last"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "played_at",
        "device",
        "listener_birth_year",
        "listener_username",
        "prev_listener_birth_year",
        "ms_played_dense_rank",
        "listener_birth_year_cume_dist",
        "last_known_ms_played",
        "listener_birth_year_sum_7",
        "prev_listener_birth_year_2",
        "played_at_quartile",
        "last_known_listener_birth_year_sum_7",
        "prev2_played_at",
        "played_at_quartile_ms_played_dense_rank_avg_play",
        F.least(F.col("ms_played_dense_rank"), F.col("last_known_ms_played")).alias("least_ms_played_dense_rank_last_known_ms_played"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
    .orderBy(F.col("listener_birth_year_sum_7").asc(), "id")
)
