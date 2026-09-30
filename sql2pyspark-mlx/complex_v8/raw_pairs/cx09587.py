from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_played_at_month = Window.partitionBy("listener_id", F.month("played_at")).orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_listener_id_by_ms_pla_2 = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_source = Window.partitionBy("device", "source")
w_source_listener_id_by_ms_pla_3 = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id")

base_plays = (
    df_plays
    .filter((F.col("ms_played") >= 168782) | (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

metrics = (
    base_plays
    .select(
        "*",
        F.nth_value("ms_played", 2).over(w_source_listener_id_by_ms_pla).alias("second_ms_played"),
        F.last("ms_played").over(w_listener_id_played_at_month).alias("last_ms_played"),
        F.first("played_at", ignorenulls=True).over(w_source_listener_id_by_ms_pla_2).alias("first_known_played_at"),
        F.size(F.collect_set("track_id").over(w_device_source)).alias("distinct_track_id_in_device_source"),
        F.row_number().over(w_source_listener_id_by_ms_pla_3).alias("rn"),
        F.max("ms_played").over(w_device_source).alias("device_source_max_ms_played"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_counts = (
    metrics.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.first_played_at",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

enriched_listener = (
    with_counts.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date").between(F.lit("2024-08-02").cast("date"), F.lit("2025-10-18").cast("date"))), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.source",
        "s.days_since_played_at",
        "s.source_slug",
        "s.last_ms_played",
        "s.first_known_played_at",
        "s.distinct_track_id_in_device_source",
        "s.rn",
        "s.device_source_max_ms_played",
        "s.min_play_ms_played",
        "s.first_played_at",
        "s.play_is_skipped_count",
        F.col("l.country").alias("listener_country"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
    )
)

with_children = (
    enriched_listener.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("phone", "car")), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source_slug",
        "s.first_known_played_at",
        "s.distinct_track_id_in_device_source",
        "s.min_play_ms_played",
        "s.first_played_at",
        "s.listener_plan",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

with_children_plays = (
    with_children.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.first_known_played_at",
        "s.first_played_at",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_played_at",
        F.col("p.id").alias("play_id_2"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    with_children_plays
    .groupBy("play_played_at", "play_played_at_2")
    .pivot("device", ["phone", "desktop", "car"])
    .agg(F.sum("play_ms_played"))
    .orderBy("play_played_at", "play_played_at_2")
)
