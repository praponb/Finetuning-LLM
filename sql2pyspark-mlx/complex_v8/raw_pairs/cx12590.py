from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_track_bpm = Window.partitionBy("source").orderBy("track_bpm", "id")
w_listener_id_source = Window.partitionBy("listener_id", "source")
w_is_skipped_played_at_year = Window.partitionBy("is_skipped", F.year("played_at"))
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id")
w_played_at_year_by_listener_s = Window.partitionBy(F.year("played_at")).orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_year_by_listener_s_2 = Window.partitionBy(F.year("played_at")).orderBy(F.col("listener_signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_played_at = Window.orderBy("last_played_at", "played_at_day", "first_listener_signup_date_month", "track_id")

filtered_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

with_details_listener = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.array_contains(F.col("l.interests"), "live"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

ranked = (
    with_details_listener
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "listener_interests",
        F.lag("listener_signup_date").over(w_source_by_track_bpm).alias("prev_listener_signup_date"),
        F.round(F.avg("track_bpm").over(w_listener_id_source), 2).alias("listener_id_source_avg_track_bpm"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_is_skipped_played_at_year), 2).alias("track_bpm_vs_is_skipped_played_at_year_avg"),
        F.round(F.col("track_bpm") / F.nullif(F.sum("track_bpm").over(w_is_skipped_played_at_year), F.lit(0)), 4).alias("track_bpm_share_of_is_skipped_played_at_year"),
        F.count(F.lit(1)).over(w_is_skipped_played_at_year).alias("rows_in_is_skipped_played_at_year"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_listener_id_source), 2).alias("track_bpm_vs_listener_id_source_avg"),
        (F.col("track_bpm") - F.lag("track_bpm").over(w_all_by_listener_signup_date)).alias("track_bpm_change"),
        F.max("track_bpm").over(w_played_at_year_by_listener_s).alias("max_track_bpm_so_far"),
        F.first("listener_signup_date").over(w_played_at_year_by_listener_s_2).alias("first_listener_signup_date"),
        F.sum("track_bpm").over(w_listener_id_source).alias("listener_id_source_sum_track_bpm"),
    )
)

derived = (
    ranked
    .filter(F.col("rows_in_is_skipped_played_at_year") > 3)
    .select(
        "*",
        F.when(F.col("max_track_bpm_so_far") >= 1452, "large").when(F.col("max_track_bpm_so_far") >= 408, "medium").otherwise("small").alias("max_track_bpm_so_far_tier"),
        (F.col("track_bpm_change") + F.col("max_track_bpm_so_far")).alias("track_bpm_change_plus_max_track_bpm_so_far"),
        F.coalesce(F.col("track_bpm_change"), F.lit(0)).alias("track_bpm_change_filled"),
    )
)

grouped = (
    derived
    .groupBy(
        F.to_date("played_at").alias("played_at_day"),
        F.date_trunc("month", "first_listener_signup_date").alias("first_listener_signup_date_month"),
        "track_id",
    )
    .agg(
        F.round(F.sum("track_bpm_share_of_is_skipped_played_at_year"), 2).alias("total_track_bpm_share_of_is_skipped_played_at_ye"),
        F.max("played_at").alias("last_played_at"),
        F.sum(F.when(~F.col("is_skipped"), F.col("listener_id_source_sum_track_bpm")).otherwise(0)).alias("listener_id_source_sum_track_bpm_not_is_skipped"),
    )
)

with_trends = (
    grouped
    .withColumn("days_since_prev_played_at_day", F.datediff(F.col("played_at_day"), F.lag("played_at_day").over(w_all_by_last_played_at)))
)

result = (
    with_trends
    .filter(F.col("track_id").isNotNull())
    .select(
        "played_at_day",
        "total_track_bpm_share_of_is_skipped_played_at_ye",
        "last_played_at",
        "listener_id_source_sum_track_bpm_not_is_skipped",
        F.to_date(F.col("last_played_at")).alias("last_played_at_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
    )
    .orderBy(F.col("listener_id_source_sum_track_bpm_not_is_skipped").asc())
)
