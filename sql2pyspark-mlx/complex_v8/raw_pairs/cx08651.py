from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_max_pl = Window.partitionBy("device", "listener_id").orderBy("max_play_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_track_duratio = Window.partitionBy("listener_id").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_plays = (
    df_plays
    .filter((F.col("listener_id") <= 18) & (F.col("device") == "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    base_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_parent_track = (
    with_counts.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select("s.*", F.col("t.duration_sec").alias("track_duration_sec"))
)

with_trends = (
    with_parent_track
    .select(
        "*",
        F.last("min_play_ms_played").over(w_device_listener_id_by_max_pl).alias("last_min_play_ms_played"),
        F.first("max_play_ms_played", ignorenulls=True).over(w_listener_id_by_track_duratio).alias("first_known_max_play_ms_played"),
    )
)

stats = (
    with_trends
    .groupBy(
        "source",
        "play_count",
        F.date_format("played_at_date", "yyyy-MM").alias("played_at_date_ym"),
    )
    .agg(
        F.round(F.avg("first_known_max_play_ms_played"), 2).alias("avg_first_known_max_play_ms_played"),
        F.count("*").alias("n_rows"),
        F.datediff(F.max("played_at_date"), F.min("played_at_date")).alias("played_at_date_span_days"),
        F.sum(F.when(F.col("source") == "search", F.col("first_known_max_play_ms_played")).otherwise(0)).alias("first_known_max_play_ms_played_source_search"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    qualified
    .select(
        "source",
        "play_count",
        "first_known_max_play_ms_played_source_search",
        "played_at_date_span_days",
        "n_rows",
    )
    .unpivot(
        ids=["source", "play_count"],
        values=["first_known_max_play_ms_played_source_search", "played_at_date_span_days", "n_rows"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("source", "play_count", "measure")
)
