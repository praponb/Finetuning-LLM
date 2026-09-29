from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id", "play_track_id").orderBy(F.col("play_played_at_month").desc(), F.col("id").desc(), F.col("play_id").desc())

filtered_plays = (
    df_plays
    .filter((F.col("device") == "car") & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_children = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.device").alias("play_device"),
    )
)

labeled = with_children.withColumn("play_played_at_month", F.month(F.col("play_played_at")))

with_parent_play = (
    labeled.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("p.listener_id"), "left")
    .filter(F.col("p.track_id") >= 5)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_ms_played",
        "s.play_played_at",
        "s.play_device",
        "s.play_played_at_month",
        F.col("p.ms_played").alias("play_ms_played_2"),
        F.col("p.track_id").alias("play_track_id"),
        F.col("l.country").alias("listener_country"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

latest = (
    with_parent_play
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched_play = (
    latest.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_ms_played",
        "s.play_played_at",
        "s.play_device",
        "s.play_ms_played_2",
        "s.play_track_id",
        "s.listener_birth_year",
        F.col("p.played_at").alias("play_played_at_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

summarized_rows = (
    enriched_play.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.play_track_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "play_id",
        "listener_birth_year",
        "play_ms_played",
        "play_is_skipped_count",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["listener_birth_year", "play_ms_played", "play_is_skipped_count"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
