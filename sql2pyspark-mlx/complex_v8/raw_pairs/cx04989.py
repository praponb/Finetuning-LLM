from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id_played_at_month = Window.partitionBy("track_id", F.month("played_at"))
w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id").rowsBetween(-6, Window.currentRow)

recent = (
    df_plays
    .filter((F.col("source").isNotNull()) & (F.col("device").isin("tv", "speaker", "car")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
        F.substring(F.trim(F.col("device")), 1, 2).alias("device_prefix"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

without_matches = (
    recent.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
    .filter(F.col("source") == "search")
)

with_details_listener = (
    without_matches.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.col("l.country").isin("JP", "KR"))
    .select("s.*", F.col("l.country").alias("listener_country"))
)

windowed = (
    with_details_listener
    .select(
        "*",
        F.sum("ms_played").over(w_track_id_played_at_month).alias("track_id_played_at_month_sum_ms_played"),
        F.round(F.avg("ms_played").over(w_listener_id_track_id_by_play), 2).alias("ms_played_avg_7"),
    )
)

result = (
    windowed
    .groupBy("played_at_hour", "track_id")
    .pivot("device", ["desktop", "tv"])
    .agg(F.sum("ms_played_avg_7").alias("amount"), F.count("ms_played_avg_7").alias("n"))
    .orderBy("played_at_hour", "track_id")
)
