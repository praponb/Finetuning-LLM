from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_hour_is_skipped = Window.partitionBy("played_at_hour", "is_skipped")
w_all_by_played_at_hour_ties = Window.orderBy(F.col("played_at_hour").asc_nulls_last())
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_all_by_played_at_hour_runnin = Window.orderBy(F.col("played_at_hour").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_prev3_played_at_full = Window.orderBy(F.col("prev3_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_prev3_played_at_prior = Window.orderBy(F.col("prev3_played_at").desc(), "id").rowsBetween(-1, -1)
w_source_is_skipped_label = Window.partitionBy("source", "is_skipped_label")
w_all_by_prev3_played_at_prior_2 = Window.orderBy(F.col("prev3_played_at").desc(), "id").rowsBetween(-3, -1)
w_all_by_played_at_hour_share = Window.orderBy(F.col("played_at_hour_share_of_played_at_hour_is_skippe").desc(), "id").rowsBetween(-6, -1)
w_latest = Window.partitionBy("listener_id").orderBy(F.col("played_at_percent_rank_max_6_prior").desc(), F.col("id").desc())

valid_plays = (
    df_plays
    .filter((F.col("source").isin("radio", "album")) | (F.col("ms_played") > 151112))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

with_trends = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        "is_skipped_label",
        F.max("played_at_hour").over(w_played_at_hour_is_skipped).alias("played_at_hour_is_skipped_max_played_at_hour"),
        F.round(F.col("played_at_hour") / F.nullif(F.sum("played_at_hour").over(w_played_at_hour_is_skipped), F.lit(0)), 4).alias("played_at_hour_share_of_played_at_hour_is_skippe"),
        F.round(F.percent_rank().over(w_all_by_played_at_hour_ties), 3).alias("played_at_hour_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.lag("played_at", 3).over(w_all_by_played_at).alias("prev3_played_at"),
        F.first("played_at", ignorenulls=True).over(w_all_by_played_at_hour_runnin).alias("first_known_played_at"),
    )
)

enriched = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_label",
        "s.played_at_hour_is_skipped_max_played_at_hour",
        "s.played_at_hour_share_of_played_at_hour_is_skippe",
        "s.played_at_percent_rank",
        "s.prev3_played_at",
        "s.first_known_played_at",
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("played_at_percent_rank").over(w_all_by_prev3_played_at_full).alias("last_played_at_percent_rank"),
        F.round(F.avg("played_at_percent_rank").over(w_all_by_prev3_played_at_prior), 2).alias("played_at_percent_rank_avg_1_prior"),
        F.count(F.lit(1)).over(w_source_is_skipped_label).alias("rows_in_source_is_skipped_label"),
        F.sum("played_at_percent_rank").over(w_all_by_prev3_played_at_prior_2).alias("played_at_percent_rank_sum_3_prior"),
        F.last("first_known_played_at").over(w_all_by_prev3_played_at_full).alias("last_first_known_played_at"),
        F.max("played_at_percent_rank").over(w_all_by_played_at_hour_share).alias("played_at_percent_rank_max_6_prior"),
    )
)

latest_per_key = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_track = (
    latest_per_key.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.played_at_hour_is_skipped_max_played_at_hour",
        "s.prev3_played_at",
        "s.first_known_played_at",
        "s.track_moods",
        "s.track_title",
        "s.played_at_percent_rank_avg_1_prior",
        "s.rows_in_source_is_skipped_label",
        "s.played_at_percent_rank_sum_3_prior",
        "s.played_at_percent_rank_max_6_prior",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods_2"),
    )
)

filtered = (
    enriched_track.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.to_date(F.col("p.played_at")) < F.lit("2025-03-08").cast("date")) & (F.col("p.device") == "tv"), "left_semi")
    .filter(F.col("played_at_percent_rank_sum_3_prior") > 0.3)
)

result = (
    filtered
    .select(
        "id",
        "track_id",
        "played_at",
        "first_known_played_at",
        "track_moods",
        "track_title",
        "played_at_percent_rank_avg_1_prior",
        "rows_in_source_is_skipped_label",
        "track_release_date",
    )
    .orderBy(F.col("track_id").asc_nulls_last(), "id")
    .limit(20)
)
