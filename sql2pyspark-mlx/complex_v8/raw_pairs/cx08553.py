from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played_ties = Window.partitionBy("source").orderBy("ms_played")
w_source_is_skipped = Window.partitionBy("source", "is_skipped")

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

summarized_rows = (
    base_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.first_played_at")
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.round(F.cume_dist().over(w_source_by_ms_played_ties), 3).alias("ms_played_cume_dist"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_source_is_skipped"),
    )
)

result = (
    metrics
    .select("id", "ms_played_share_of_source_is_skipped", "ms_played_cume_dist")
    .unpivot(
        ids=["id"],
        values=["ms_played_share_of_source_is_skipped", "ms_played_cume_dist"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
