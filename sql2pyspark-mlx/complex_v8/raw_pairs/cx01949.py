from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_label = Window.partitionBy("is_skipped_label")
w_played_at_week_month_by_avg = Window.partitionBy(F.date_trunc("month", "played_at_week")).orderBy("avg_ms_played", "id").rowsBetween(-2, Window.currentRow)
w_source_played_at_week_year = Window.partitionBy("source", F.year("played_at_week"))
w_all_by_listener_signup_date = Window.orderBy("listener_signup_date", "id")
w_all_by_listener_signup_date_2 = Window.orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.date_trunc("week", F.col("played_at")).alias("played_at_week"),
    )
)

device_stats = (
    scoped_plays
    .groupBy("device")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

vs_group = (
    scoped_plays.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

with_windows = (
    vs_group
    .select(
        "*",
        F.round(F.avg("ms_played_minus_avg").over(w_is_skipped_label), 2).alias("is_skipped_label_avg_ms_played_minus_avg"),
        F.round(F.col("avg_ms_played") / F.nullif(F.sum("avg_ms_played").over(w_is_skipped_label), F.lit(0)), 4).alias("avg_ms_played_share_of_is_skipped_label"),
        F.max("ms_played_minus_avg").over(w_is_skipped_label).alias("is_skipped_label_max_ms_played_minus_avg"),
        F.max("ms_played_minus_avg").over(w_played_at_week_month_by_avg).alias("ms_played_minus_avg_max_3"),
    )
)

joined = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.array_contains(F.col("l.interests"), "indie"))
    .select(
        "s.*",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.signup_date").alias("listener_signup_date"),
    )
)

joined_2 = (
    joined.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_label",
        "s.played_at_week",
        "s.avg_ms_played",
        "s.ms_played_minus_avg",
        "s.is_skipped_label_avg_ms_played_minus_avg",
        "s.avg_ms_played_share_of_is_skipped_label",
        "s.ms_played_minus_avg_max_3",
        "s.listener_birth_year",
        "s.listener_interests",
        "s.listener_signup_date",
        F.col("l.signup_date").alias("listener_signup_date_2"),
        F.col("l.plan").alias("listener_plan"),
    )
)

with_parent_track = (
    joined_2.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select("s.*", F.col("t.artist_id").alias("track_artist_id"))
)

sequenced = (
    with_parent_track
    .select(
        "*",
        F.max("ms_played_minus_avg").over(w_source_played_at_week_year).alias("source_played_at_week_year_max_ms_played_minus_a"),
        F.round(F.percent_rank().over(w_all_by_listener_signup_date), 3).alias("listener_signup_date_percent_rank"),
        F.count(F.lit(1)).over(w_source_played_at_week_year).alias("rows_in_source_played_at_week_year"),
        F.min("ms_played_minus_avg_max_3").over(w_all_by_listener_signup_date_2).alias("min_ms_played_minus_avg_max_3_so_far"),
        F.max("avg_ms_played").over(w_source_played_at_week_year).alias("source_played_at_week_year_max_avg_ms_played"),
        F.lag("avg_ms_played", 1, 0).over(w_all_by_listener_signup_date).alias("prev_avg_ms_played"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "is_skipped_label",
        "avg_ms_played",
        "ms_played_minus_avg",
        "avg_ms_played_share_of_is_skipped_label",
        "ms_played_minus_avg_max_3",
        "listener_birth_year",
        "listener_interests",
        "listener_signup_date",
        "listener_signup_date_2",
        "listener_plan",
        "source_played_at_week_year_max_ms_played_minus_a",
        "rows_in_source_played_at_week_year",
        "min_ms_played_minus_avg_max_3_so_far",
        "source_played_at_week_year_max_avg_ms_played",
        "prev_avg_ms_played",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at_week")).alias("days_since_played_at_week"),
    )
    .orderBy(F.col("ms_played_minus_avg_max_3").desc())
)
