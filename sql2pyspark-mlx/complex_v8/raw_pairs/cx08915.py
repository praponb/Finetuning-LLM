from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id_by_play = Window.partitionBy("listener_id", "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source = Window.partitionBy("source")

scoped_plays = (
    df_plays
    .filter((F.col("ms_played") <= 225733) | (F.col("played_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

track_id_stats = (
    scoped_plays
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

above_average = (
    scoped_plays.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

windowed = (
    above_average
    .withColumn("second_ms_played", F.nth_value("ms_played", 2).over(w_listener_id_track_id_by_play))
    .withColumn("source_sum_ms_played_minus_avg", F.sum("ms_played_minus_avg").over(w_source))
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.min("ms_played").alias("min_play_ms_played"))
)

summarized_rows = (
    windowed.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.min_play_ms_played")
)

result = (
    summarized_rows
    .groupingSets(
        [["device", "source"], ["device"], ["source"], []],
        "device", "source",
    )
    .agg(
        F.round(F.avg("avg_ms_played"), 2).alias("avg_avg_ms_played"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
    .orderBy(F.col("device").asc_nulls_last(), F.col("source").asc_nulls_last())
)
