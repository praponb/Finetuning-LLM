from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")

scoped = (
    df_plays
    .filter((F.col("device") != "phone") & (F.to_date(F.col("played_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.lower(F.trim(F.col("source"))).alias("source_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"))
)

with_stats = (
    scoped.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.avg_play_ms_played")
)

qualified = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (~F.col("p.is_skipped")) & (F.col("p.listener_id") >= 9), "left_semi")
)

metrics = (
    qualified
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "avg_play_ms_played",
        F.round(F.avg("ms_played_bucket").over(w_listener_id), 2).alias("listener_id_avg_ms_played_bucket"),
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
    )
)

result = (
    metrics
    .select("id", "ms_played", "listener_id_max_ms_played")
    .unpivot(
        ids=["id"],
        values=["ms_played", "listener_id_max_ms_played"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
