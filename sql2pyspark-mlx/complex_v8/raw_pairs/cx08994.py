from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_id_by_ms_played_filled = Window.partitionBy("play_id").orderBy("ms_played_filled", "id", "play_id")
w_play_listener_id = Window.partitionBy("play_listener_id")
w_play_id_by_ms_played_filled_2 = Window.partitionBy("play_id").orderBy("ms_played_filled")

recent_plays = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        (F.col("played_at") + F.expr("INTERVAL 15 MINUTES")).alias("played_at_plus_15m"),
    )
)

with_children_plays = (
    recent_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.ms_played_filled",
        "s.played_at_plus_15m",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

sequenced = (
    with_children_plays
    .select(
        "*",
        F.row_number().over(w_play_id_by_ms_played_filled).alias("ms_played_filled_seq"),
        F.round(F.avg("ms_played_filled").over(w_play_listener_id), 2).alias("play_listener_id_avg_ms_played_filled"),
        F.sum("ms_played_filled").over(w_play_listener_id).alias("play_listener_id_sum_ms_played_filled"),
        F.rank().over(w_play_id_by_ms_played_filled_2).alias("ms_played_filled_rank"),
        F.count(F.lit(1)).over(w_play_listener_id).alias("rows_in_play_listener_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "play_id",
        "ms_played_filled",
        "rows_in_play_listener_id",
        "ms_played_filled_seq",
        "ms_played",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["ms_played_filled", "rows_in_play_listener_id", "ms_played_filled_seq", "ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
