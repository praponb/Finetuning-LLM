from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id", "listener_id").orderBy("play_source_count", "id")
w_latest_2 = Window.partitionBy("listener_id").orderBy("played_at", "id")

scoped = (
    df_plays
    .filter(F.col("device") == "car")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source").isin("playlist", "search", "radio"), 1)).alias("play_source_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.last_played_at",
    )
)

latest = (
    summarized_rows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    latest
    .withColumn("rn", F.row_number().over(w_latest_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    unique_rows
    .select("id", "play_source_count", "ms_played")
    .unpivot(
        ids=["id"],
        values=["play_source_count", "ms_played"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
