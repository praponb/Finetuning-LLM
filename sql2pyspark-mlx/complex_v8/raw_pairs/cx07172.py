from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_source_radi = Window.orderBy(F.col("ms_played_source_radio").desc(), "track_id", "played_at_month")
w_played_at_month_month_by_las = Window.partitionBy(F.month("played_at_month")).orderBy(F.col("last_played_at").desc())
w_dedup = Window.partitionBy("play_device", "play_listener_id").orderBy("last_played_at", "track_id", "played_at_month", "play_id")

base_plays = (
    df_plays
    .filter(F.col("ms_played") > 52009)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
    )
)

per_group = (
    base_plays
    .filter(F.col("track_id") >= 3)
    .groupBy("track_id", F.date_trunc("month", "played_at").alias("played_at_month"))
    .agg(
        F.sum(F.when(F.col("source") == "playlist", 1).otherwise(0)).alias("n_source_playlist"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("n_source_is_not_null"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.sum(F.when(F.col("source") == "radio", F.col("ms_played")).otherwise(0)).alias("ms_played_source_radio"),
    )
)

ranked = (
    per_group
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_ms_played_source_radi), 3).alias("ms_played_source_radio_percent_rank"),
        F.rank().over(w_played_at_month_month_by_las).alias("last_played_at_rank"),
    )
)

with_children_plays = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.track_id",
        "s.played_at_month",
        "s.source_list",
        "s.last_played_at",
        "s.avg_ms_played",
        "s.ms_played_source_radio",
        "s.last_played_at_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
    )
)

latest = (
    with_children_plays
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

derived = (
    latest
    .select(
        "track_id",
        "played_at_month",
        "source_list",
        "last_played_at",
        "avg_ms_played",
        "last_played_at_rank",
        "play_id",
        "play_device",
        F.upper(F.trim(F.col("play_device"))).alias("play_device_clean"),
        F.hour(F.col("last_played_at")).alias("last_played_at_hour"),
    )
)

result = (
    derived
    .select(
        "track_id",
        "source_list",
        "last_played_at_rank",
        "play_id",
        "play_device",
        "play_device_clean",
        (F.col("last_played_at_rank") + F.col("last_played_at_hour")).alias("last_played_at_rank_plus_last_played_at_hour"),
    )
    .orderBy(F.col("play_device_clean").desc(), "play_device")
)
