from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_source_track_id_by_played_at = Window.partitionBy("source", "track_id").orderBy("played_at", "id")
w_first_played_at_month_distin = Window.partitionBy(F.date_trunc("month", "first_played_at"), "distinct_device_in_listener_id").orderBy("distinct_device_in_listener_id")
w_first_played_at_month_distin_2 = Window.partitionBy(F.date_trunc("month", "first_played_at"), "distinct_device_in_listener_id").orderBy("distinct_device_in_listener_id", "id")
w_distinct_device_in_listener = Window.partitionBy("distinct_device_in_listener_id")
w_first_played_at_month_distin_3 = Window.partitionBy(F.date_trunc("month", "first_played_at"), "distinct_device_in_listener_id").orderBy("distinct_device_in_listener_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_plays
    .filter((F.col("ms_played").between(92277, 287769)) & (F.col("track_id").isNotNull()))
)

with_parent = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.release_date").isNotNull())
    .select("s.*", F.col("t.release_date").alias("track_release_date"))
)

combined_details = (
    with_parent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.release_date").alias("track_release_date_2"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "listener_id",
        "ms_played",
        "source",
        "track_duration_sec",
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        (F.col("ms_played") - F.lag("ms_played").over(w_source_track_id_by_played_at)).alias("ms_played_change"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

profiled = (
    with_trends.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at")
)

windowed = (
    profiled
    .select(
        "id",
        "listener_id",
        "source",
        "track_duration_sec",
        "distinct_device_in_listener_id",
        F.dense_rank().over(w_first_played_at_month_distin).alias("distinct_device_in_listener_id_dense_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_first_played_at_month_distin_2)).alias("ms_played_change_2"),
        F.lag("track_duration_sec", 2).over(w_first_played_at_month_distin_2).alias("prev2_track_duration_sec"),
        F.max("ms_played_change").over(w_distinct_device_in_listener).alias("distinct_device_in_listener_id_max_ms_played_cha"),
        F.last("track_duration_sec", ignorenulls=True).over(w_first_played_at_month_distin_3).alias("last_known_track_duration_sec"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (~F.col("p.is_skipped")) & (F.month(F.col("p.played_at")).isin(1, 2)), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "listener_id",
        "source",
        "track_duration_sec",
        "distinct_device_in_listener_id_dense_rank",
        "ms_played_change_2",
        "distinct_device_in_listener_id_max_ms_played_cha",
        "last_known_track_duration_sec",
        F.when(F.col("source").isin("search", "radio"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy(F.col("listener_id").desc(), F.col("source_group").asc())
)
