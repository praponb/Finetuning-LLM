from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_played_at_quarter = Window.partitionBy("device").orderBy(F.col("played_at_quarter").desc(), "id")
w_distinct_play_sources_by_las = Window.partitionBy("distinct_play_sources").orderBy(F.col("last_play_played_at").desc_nulls_first(), "device", "play_track_id", "prev3_played_at_month")
w_distinct_play_sources_prev3 = Window.partitionBy("distinct_play_sources", F.date_trunc("month", "prev3_played_at_month"))
w_play_track_id_by_distinct_pl = Window.partitionBy("play_track_id").orderBy("distinct_play_sources", "device", "play_track_id", "prev3_played_at_month").rowsBetween(-6, -1)

filtered = (
    df_plays
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.upper(F.trim(F.col("source"))).alias("source_clean"),
        F.date_trunc("quarter", F.col("played_at")).alias("played_at_quarter"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.length(F.trim(F.col("l.username"))) >= 8) & (F.col("l.country") != "UK"), "left_semi")
)

sequenced = (
    without_matches
    .select(
        "id",
        "listener_id",
        "played_at",
        "device",
        "is_skipped",
        "source_clean",
        "played_at_quarter",
        F.first("played_at", ignorenulls=True).over(w_track_id_is_skipped_by_playe).alias("first_known_played_at"),
        F.lag("played_at", 3).over(w_device_by_played_at_quarter).alias("prev3_played_at"),
    )
)

detail_rows_plays = (
    sequenced.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.first_known_played_at",
        "s.prev3_played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

profiled = (
    detail_rows_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id"), "left")
    .select("s.*", "p.last_played_at")
)

per_group = (
    profiled
    .groupBy(
        "device",
        "play_track_id",
        F.date_trunc("month", "prev3_played_at").alias("prev3_played_at_month"),
    )
    .agg(
        F.max("play_played_at").alias("last_play_played_at"),
        F.countDistinct("play_source").alias("distinct_play_sources"),
    )
)

sequenced_2 = (
    per_group
    .select(
        "device",
        "play_track_id",
        "prev3_played_at_month",
        "last_play_played_at",
        "distinct_play_sources",
        F.lag("last_play_played_at").over(w_distinct_play_sources_by_las).alias("prev_last_play_played_at"),
        F.round(F.col("distinct_play_sources") - F.avg("distinct_play_sources").over(w_distinct_play_sources_prev3), 2).alias("distinct_play_sources_vs_distinct_play_sources_p"),
        F.round(F.avg("distinct_play_sources").over(w_play_track_id_by_distinct_pl), 2).alias("distinct_play_sources_avg_6_prior"),
    )
)

result = (
    sequenced_2
    .select(
        "device",
        "play_track_id",
        "last_play_played_at",
        "prev_last_play_played_at",
        "distinct_play_sources_vs_distinct_play_sources_p",
        "distinct_play_sources_avg_6_prior",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
    )
    .orderBy(
        F.col("device_clean").desc_nulls_first(),
        F.col("prev_last_play_played_at").desc(),
    )
)
