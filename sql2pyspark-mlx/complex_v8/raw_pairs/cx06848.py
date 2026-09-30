from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_played_at_month_hour_pl = Window.partitionBy("play_played_at_month_hour", "play_id").orderBy("total_play_ms_played_per_avg_source_len", "play_id", "play_played_at_month").rowsBetween(-6, Window.currentRow)
w_play_played_at_month_to_date = Window.partitionBy(F.to_date("play_played_at_month")).orderBy(F.col("play_played_at_month_hour").desc())
w_play_played_at_month_hour_pl_2 = Window.partitionBy("play_played_at_month_hour", "play_id").orderBy("total_play_ms_played_per_avg_source_len", "play_id", "play_played_at_month")

valid_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.when(F.col("device").isin("tv", "speaker"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
)

with_children = (
    valid_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device") != "speaker"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.source",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

labeled = (
    with_children
    .select(
        "id",
        "listener_id",
        "played_at",
        "source",
        "play_id",
        "play_device",
        "play_ms_played",
        "play_played_at",
        F.coalesce(F.col("play_source"), F.lit("UNKNOWN")).alias("play_source_or_unknown"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("play_played_at")).alias("days_since_play_played_at"),
    )
)

totals = (
    labeled
    .groupBy(
        "play_id",
        F.date_trunc("month", "play_played_at").alias("play_played_at_month"),
    )
    .agg(
        F.round(F.avg("source_len"), 2).alias("avg_source_len"),
        F.sum("play_ms_played").alias("total_play_ms_played"),
    )
    .filter(F.col("total_play_ms_played") > 92277)
)

annotated = (
    totals
    .filter(F.col("total_play_ms_played") > 843250)
    .select(
        "play_id",
        "play_played_at_month",
        "avg_source_len",
        F.round(F.col("total_play_ms_played") / F.nullif(F.col("avg_source_len"), F.lit(0)), 3).alias("total_play_ms_played_per_avg_source_len"),
        F.hour(F.col("play_played_at_month")).alias("play_played_at_month_hour"),
        F.round(F.col("total_play_ms_played") / F.nullif(F.col("avg_source_len"), F.lit(0)), 3).alias("total_play_ms_played_per_avg_source_len_2"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.round(F.avg("play_played_at_month_hour").over(w_play_played_at_month_hour_pl), 2).alias("play_played_at_month_hour_avg_7"),
        F.round(F.percent_rank().over(w_play_played_at_month_to_date), 3).alias("play_played_at_month_hour_percent_rank"),
        (F.col("avg_source_len") - F.lag("avg_source_len").over(w_play_played_at_month_hour_pl_2)).alias("avg_source_len_change"),
    )
)

result = (
    with_windows
    .select(
        "play_id",
        "play_played_at_month_hour_percent_rank",
        "play_played_at_month",
    )
    .distinct()
    .orderBy(F.col("play_id").asc_nulls_last())
)
