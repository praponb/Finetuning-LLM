from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), F.col("id").desc())

in_window = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.ms_played").alias("prior_ms_played_1d"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
    )
)

with_counts = (
    in_window.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

latest = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

totals = (
    latest
    .filter(F.col("play_count_2") <= 5)
    .groupBy("prior_rows_1d", "play_count")
    .agg(
        F.count("*").alias("row_count"),
        F.min("play_count").alias("min_play_count"),
        F.min("played_at").alias("first_played_at_2"),
        F.min("first_played_at").alias("first_first_played_at"),
        F.max("first_played_at").alias("last_first_played_at"),
        F.round(F.sum("prior_rows_1d") / F.nullif(F.sum("prior_ms_played_1d"), F.lit(0)), 3).alias("prior_rows_1d_to_prior_ms_played_1d_ratio"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    totals
    .select(
        "prior_rows_1d",
        "play_count",
        "min_play_count",
        "first_played_at_2",
        "first_first_played_at",
        "last_first_played_at",
        F.date_trunc("quarter", F.col("last_first_played_at")).alias("last_first_played_at_quarter"),
    )
    .orderBy("min_play_count")
)
