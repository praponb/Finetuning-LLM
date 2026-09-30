from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_prior_6 = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_ms_played_tier = Window.partitionBy("ms_played_tier")
w_all_by_played_at_prior_3 = Window.orderBy(F.col("played_at").desc_nulls_first(), "id").rowsBetween(-3, -1)

recent = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.when(F.col("ms_played") >= 241585, "HIGH").when(F.col("ms_played") >= 168782, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.sum("ms_played").alias("total_play_ms_played"),
        F.max("ms_played").alias("max_play_ms_played"),
    )
)

with_counts = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        "p.total_play_ms_played",
        "p.max_play_ms_played",
    )
)

metrics = (
    with_counts
    .select(
        "*",
        F.sum("total_play_ms_played").over(w_all_by_played_at_prior_6).alias("total_play_ms_played_sum_6_prior"),
        F.row_number().over(w_all_by_played_at).alias("played_at_seq"),
        F.size(F.collect_set("listener_id").over(w_ms_played_tier)).alias("distinct_listener_id_in_ms_played_tier"),
        F.round(F.avg("max_play_ms_played").over(w_all_by_played_at_prior_3), 2).alias("max_play_ms_played_avg_3_prior"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.country").isin("KR", "TH", "UK")), "left_semi")
)

first_rows = (
    without_matches
    .filter(F.col("played_at_seq") == 1)
    .drop("played_at_seq")
)

result = (
    first_rows
    .groupBy("played_at", "track_id")
    .pivot("source", ["search", "album", "radio", "playlist"])
    .agg(F.min("ms_played"))
    .orderBy("played_at", "track_id")
)
