from pyspark.sql import functions as F

covered = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

with_children_plays = (
    covered.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.ms_played") > 92277), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.played_at",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    with_children_plays
    .select(
        "id",
        "listener_id",
        "played_at",
        "play_id",
        "play_is_skipped",
        F.lower(F.regexp_replace(F.trim(F.col("play_source")), " ", "_")).alias("play_source_slug"),
        F.concat_ws("-", F.col("play_device"), F.col("play_source")).alias("play_device_play_source_label"),
    )
    .orderBy(F.col("play_device_play_source_label").desc())
)
