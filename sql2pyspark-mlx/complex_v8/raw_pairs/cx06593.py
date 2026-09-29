from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_dedup = Window.partitionBy("source").orderBy(F.col("rn").desc(), F.col("id").desc())

valid_plays = (
    df_plays
    .filter((F.col("is_skipped")) & (~F.col("source").isin("album", "playlist")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

ranked = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.rank().over(w_all_by_played_at_ties).alias("played_at_rank"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
    )
)

top_rows = ranked.filter(F.col("rn") == 1)

unique_rows = (
    top_rows
    .withColumn("rn_2", F.row_number().over(w_dedup))
    .filter(F.col("rn_2") == 1)
    .drop("rn_2")
)

line_level_plays = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.rn",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
        F.min("played_at").alias("first_played_at"),
    )
)

summarized_rows = (
    line_level_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
        "p.first_played_at",
    )
)

screened = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.avg("ms_played").alias("overall_avg_ms_played")))
    .filter(F.col("ms_played") > F.col("overall_avg_ms_played"))
    .drop("overall_avg_ms_played")
)

result = (
    screened
    .filter(F.col("play_ms_played") > 129336)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "rn",
        "play_id",
        "play_ms_played",
        "play_played_at",
        "play_count",
        "last_played_at",
        "first_played_at",
        F.round((F.unix_timestamp(F.col("last_played_at")) - F.unix_timestamp(F.col("first_played_at"))) / 60, 1).alias("minutes_first_played_at_to_last_played_at"),
    )
    .orderBy("rn", F.col("minutes_first_played_at_to_last_played_at").desc())
)
