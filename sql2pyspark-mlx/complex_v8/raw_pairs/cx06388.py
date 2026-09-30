from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_ms_played_runnin = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc())
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_track_is_explicit = Window.partitionBy("track_is_explicit")
w_all_by_ms_played_moving_1 = Window.orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_played_at_rank = Window.partitionBy("played_at_rank")
w_played_at_dense_rank_by_ms_p = Window.partitionBy("played_at_dense_rank").orderBy("ms_played_avg_2", "id").rowsBetween(-6, Window.currentRow)

recent_plays = (
    df_plays
    .filter(F.col("track_id") >= 28)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
)

with_parent = (
    recent_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.is_skipped",
        "s.device_clean",
        F.col("l.country").alias("listener_country"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "device_clean",
        "listener_country",
        "track_is_explicit",
        F.first("ms_played").over(w_all_by_played_at_full).alias("first_ms_played"),
        F.max("ms_played").over(w_track_id_by_ms_played_runnin).alias("max_ms_played_so_far"),
        F.round(F.cume_dist().over(w_listener_id_by_ms_played_tie), 3).alias("ms_played_cume_dist"),
        F.rank().over(w_all_by_played_at_ties).alias("played_at_rank"),
        F.ntile(10).over(w_all_by_played_at).alias("played_at_decile"),
        F.ntile(5).over(w_listener_id_by_ms_played).alias("ms_played_quintile"),
        F.dense_rank().over(w_all_by_played_at_ties).alias("played_at_dense_rank"),
        F.count(F.lit(1)).over(w_track_is_explicit).alias("rows_in_track_is_explicit"),
        F.nth_value("ms_played", 2).over(w_all_by_played_at_full).alias("second_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_track_id_by_ms_played_runnin).alias("first_known_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_1), 2).alias("ms_played_avg_2"),
    )
)

scored = (
    ranked
    .filter(F.col("max_ms_played_so_far") > 1603000)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "listener_country",
        "track_is_explicit",
        "ms_played_cume_dist",
        "played_at_rank",
        "ms_played_quintile",
        "played_at_dense_rank",
        "rows_in_track_is_explicit",
        "second_ms_played",
        "first_known_ms_played",
        "ms_played_avg_2",
        F.round(F.col("ms_played_cume_dist") * 0.9, 2).alias("ms_played_cume_dist_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.substring(F.trim(F.col("device_clean")), 1, 3).alias("device_clean_prefix"),
    )
)

windowed = (
    scored
    .select(
        "*",
        F.count(F.lit(1)).over(w_played_at_rank).alias("rows_in_played_at_rank"),
        F.max("first_known_ms_played").over(w_played_at_dense_rank_by_ms_p).alias("first_known_ms_played_max_7"),
        F.max("days_since_played_at").over(w_played_at_rank).alias("played_at_rank_max_days_since_played_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "track_is_explicit",
        "ms_played_cume_dist",
        "played_at_rank",
        "ms_played_quintile",
        "played_at_dense_rank",
        "rows_in_track_is_explicit",
        "second_ms_played",
        "first_known_ms_played",
        "ms_played_cume_dist_adj",
        "device_clean_prefix",
        "rows_in_played_at_rank",
        "first_known_ms_played_max_7",
        F.when(F.col("listener_country").isin("KR", "TH"), "group_a").when(F.col("listener_country") == "UK", "group_b").otherwise("other").alias("listener_country_group"),
    )
    .orderBy("played_at_rank", F.col("rows_in_track_is_explicit").desc_nulls_first())
)
