from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_played_at_month_track_i = Window.partitionBy(F.date_trunc("month", "last_played_at"), "track_id").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_played_at_to_date_ms_pl = Window.partitionBy(F.to_date("last_played_at"), "ms_played_tier")
w_played_at_to_date_source_gro = Window.partitionBy(F.to_date("played_at"), "source_group").orderBy(F.col("played_at").desc(), "id")

filtered = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.col("listener_id") <= 14))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.when(F.col("source").isin("playlist", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("ms_played") >= 225733, "HIGH").when(F.col("ms_played") >= 73335, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count(F.when(F.col("source") == "playlist", 1)).alias("play_source_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
        "p.last_played_at",
    )
)

sequenced = (
    summarized_rows
    .select(
        "*",
        F.last("play_count").over(w_last_played_at_month_track_i).alias("last_play_count"),
        F.round(F.avg("play_source_count").over(w_last_played_at_to_date_ms_pl), 2).alias("last_played_at_to_date_ms_played_tier_avg_play_s"),
        F.round(F.cume_dist().over(w_played_at_to_date_source_gro), 3).alias("played_at_cume_dist"),
    )
)

result = (
    sequenced
    .groupBy("play_source_count", "played_at_date")
    .pivot("ms_played_tier", ["HIGH", "MEDIUM", "LOW"])
    .agg(F.avg("last_play_count").alias("avg_v"), F.count("last_play_count").alias("n"))
    .orderBy("play_source_count", "played_at_date")
)
