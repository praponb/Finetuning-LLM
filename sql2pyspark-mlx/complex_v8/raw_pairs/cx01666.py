from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_played = Window.partitionBy("source", "listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id")
w_listener_id_by_played_at_ful = Window.partitionBy("listener_id").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_is_skipped_true_pri = Window.orderBy("n_is_skipped_true", "second_played_at_month", "listener_id").rowsBetween(-3, -1)
w_all_by_n_is_skipped_true_tie = Window.orderBy("n_is_skipped_true")
w_second_played_at_month_year = Window.partitionBy(F.year("second_played_at_month"), "n_is_skipped_true").orderBy(F.col("distinct_listener_ids").desc())
w_all_by_n_is_skipped_true_run = Window.orderBy("n_is_skipped_true", "second_played_at_month", "listener_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_plays
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 220498, "A").when(F.col("ms_played") >= 92277, "B").otherwise("C").alias("ms_played_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

windowed = (
    cleaned
    .select(
        "id",
        "listener_id",
        "played_at",
        "is_skipped",
        "played_at_label",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_source_listener_id_by_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.nth_value("played_at", 2).over(w_listener_id_by_played_at_ful).alias("second_played_at"),
    )
)

grouped = (
    windowed
    .groupBy(
        F.date_trunc("month", "second_played_at").alias("second_played_at_month"),
        "listener_id",
    )
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true"),
    )
)

combined_details = (
    grouped.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.username").isNotNull()), "left")
    .select(
        "s.second_played_at_month",
        "s.listener_id",
        "s.distinct_listener_ids",
        "s.n_is_skipped_true",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
    )
)

with_windows = (
    combined_details
    .select(
        "second_played_at_month",
        "listener_id",
        "distinct_listener_ids",
        "n_is_skipped_true",
        "listener_username",
        F.sum("distinct_listener_ids").over(w_all_by_n_is_skipped_true_pri).alias("distinct_listener_ids_sum_3_prior"),
        F.dense_rank().over(w_all_by_n_is_skipped_true_tie).alias("n_is_skipped_true_dense_rank"),
        F.round(F.percent_rank().over(w_second_played_at_month_year), 3).alias("distinct_listener_ids_percent_rank"),
        F.first("distinct_listener_ids", ignorenulls=True).over(w_all_by_n_is_skipped_true_run).alias("first_known_distinct_listener_ids"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.hour(F.col("second_played_at_month")).alias("second_played_at_month_hour"),
        F.round(F.col("distinct_listener_ids") / F.nullif(F.col("n_is_skipped_true"), F.lit(0)), 3).alias("distinct_listener_ids_per_n_is_skipped_true"),
        F.round(F.col("n_is_skipped_true") / F.nullif(F.col("distinct_listener_ids"), F.lit(0)), 3).alias("n_is_skipped_true_per_distinct_listener_ids"),
    )
)

exploded_detail_plays = (
    labeled.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.second_played_at_month",
        "s.listener_id",
        "s.distinct_listener_ids",
        "s.n_is_skipped_true",
        "s.listener_username",
        "s.n_is_skipped_true_dense_rank",
        "s.first_known_distinct_listener_ids",
        "s.second_played_at_month_hour",
        "s.distinct_listener_ids_per_n_is_skipped_true",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    exploded_detail_plays
    .filter(F.col("n_is_skipped_true_dense_rank") <= 7)
    .select(
        "second_played_at_month",
        "distinct_listener_ids",
        "n_is_skipped_true_dense_rank",
        "play_id",
        "play_is_skipped",
        "play_track_id",
        "play_ms_played",
        "play_source",
    )
    .orderBy(F.col("play_track_id").asc_nulls_last())
)
