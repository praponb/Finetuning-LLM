from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("track_id").orderBy(F.col("played_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("source").orderBy("played_at", "id")

base_plays = (
    df_plays
    .filter((F.col("source") != "radio") & (~F.col("device").isin("phone", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
    )
)

deduped = (
    base_plays
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

summary = (
    unique_rows
    .groupBy("track_id", "device")
    .agg(
        F.count(F.when(F.col("source") == "album", 1)).alias("n_source_album"),
        F.round(100 * F.count(F.when(F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_is_skipped_true"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("ms_played").alias("min_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    summary.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.min_play_ms_played",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

joined_track = (
    profiled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.track_id",
        "s.device",
        "s.n_source_album",
        "s.pct_is_skipped_true",
        "s.min_play_ms_played",
        "s.play_count",
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    joined_track
    .select(
        "track_id",
        "device",
        "track_bpm",
        "play_count",
    )
    .unpivot(
        ids=["track_id", "device"],
        values=["track_bpm", "play_count"],
        variableColumnName="metric_name",
        valueColumnName="amount",
    )
    .orderBy("track_id", "device", "metric_name")
)
