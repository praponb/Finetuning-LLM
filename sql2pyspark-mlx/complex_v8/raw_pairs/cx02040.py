from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_last_played_at_mo = Window.partitionBy("is_skipped", F.date_trunc("month", "last_played_at")).orderBy(F.col("max_play_ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_count_track_id_by_ms_pl = Window.partitionBy("play_count", "track_id").orderBy("ms_played", "id")
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_is_skipped_last_played_at_mo_2 = Window.partitionBy("is_skipped", F.date_trunc("month", "last_played_at")).orderBy(F.col("max_play_ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_played_at = Window.orderBy("last_played_at", "id")
w_play_count = Window.partitionBy("play_count")

prepared_plays = (
    df_plays
    .filter((~F.col("is_skipped")) & (F.hour(F.col("played_at")).between(10, 14)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 168782).alias("ms_played_distance"),
        F.date_trunc("year", F.col("played_at")).alias("played_at_year"),
        F.year(F.col("played_at")).alias("played_at_year_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

summarized_rows = (
    prepared_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

with_trends = (
    summarized_rows
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        "ms_played_distance",
        "played_at_year",
        "played_at_year_2",
        "play_count",
        "last_played_at",
        F.first("play_count", ignorenulls=True).over(w_is_skipped_last_played_at_mo).alias("first_known_play_count"),
        F.lag("play_count").over(w_play_count_track_id_by_ms_pl).alias("prev_play_count"),
        F.max("played_at_year_2").over(w_device_listener_id).alias("device_listener_id_max_played_at_year_2"),
        F.last("ms_played_distance").over(w_is_skipped_last_played_at_mo_2).alias("last_ms_played_distance"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.ntile(3).over(w_all_by_last_played_at).alias("last_played_at_tercile"),
        F.round(F.col("first_known_play_count") / F.nullif(F.sum("first_known_play_count").over(w_play_count), F.lit(0)), 4).alias("first_known_play_count_share_of_play_count"),
        F.lag("first_known_play_count", 3).over(w_all_by_last_played_at).alias("prev3_first_known_play_count"),
    )
)

result = (
    metrics
    .filter(F.col("device_listener_id_max_played_at_year_2") >= 5671)
    .select(
        "id",
        "device",
        "is_skipped",
        "source",
        "played_at_year",
        "play_count",
        "last_played_at",
        "device_listener_id_max_played_at_year_2",
        "last_ms_played_distance",
        "first_known_play_count_share_of_play_count",
        "prev3_first_known_play_count",
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
    )
)
