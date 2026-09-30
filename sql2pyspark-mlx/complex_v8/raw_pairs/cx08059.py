from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_is_skipped_by_li = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("listener_birth_year").asc_nulls_last(), "id")
w_track_id = Window.partitionBy("track_id")
w_device_listener_id_by_listen = Window.partitionBy("device", "listener_id").orderBy("listener_birth_year", "id")
w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_device_listener_id = Window.partitionBy("device", "listener_id")
w_is_skipped_by_listener_birth = Window.partitionBy("is_skipped").orderBy("listener_birth_year", "id")
w_listener_id = Window.partitionBy("listener_id")
w_track_id_by_rows_in_device_i = Window.partitionBy("track_id").orderBy("rows_in_device_is_skipped", "id")
w_device_by_listener_birth_yea = Window.partitionBy("device").orderBy(F.col("listener_birth_year_share_of_track_id").desc())

cleaned = (
    df_plays
    .filter(F.col("ms_played") >= 160264)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.rank().over(w_listener_id_is_skipped_by_li).alias("listener_birth_year_rank"),
        F.round(F.col("listener_birth_year") / F.nullif(F.sum("listener_birth_year").over(w_track_id), F.lit(0)), 4).alias("listener_birth_year_share_of_track_id"),
        F.lag("listener_birth_year", 2).over(w_listener_id_is_skipped_by_li).alias("prev2_listener_birth_year"),
        F.lag("listener_birth_year", 2, 0).over(w_device_listener_id_by_listen).alias("prev2_listener_birth_year_2"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_device_is_skipped), 2).alias("listener_birth_year_vs_device_is_skipped_avg"),
        F.min("listener_birth_year").over(w_device_listener_id).alias("device_listener_id_min_listener_birth_year"),
        (F.col("listener_birth_year") - F.lag("listener_birth_year").over(w_is_skipped_by_listener_birth)).alias("listener_birth_year_change"),
        F.count(F.lit(1)).over(w_device_is_skipped).alias("rows_in_device_is_skipped"),
    )
)

tagged = (
    windowed
    .withColumn("listener_birth_year_per_listener_birth_year_rank", F.round(F.col("listener_birth_year") / F.nullif(F.col("listener_birth_year_rank"), F.lit(0)), 3))
)

with_windows = (
    tagged
    .select(
        "*",
        F.round(F.avg("listener_birth_year_rank").over(w_listener_id), 2).alias("listener_id_avg_listener_birth_year_rank"),
        F.lag("listener_birth_year_change").over(w_track_id_by_rows_in_device_i).alias("prev_listener_birth_year_change"),
        F.dense_rank().over(w_device_by_listener_birth_yea).alias("listener_birth_year_share_of_track_id_dense_rank"),
        F.round(F.percent_rank().over(w_device_by_listener_birth_yea), 3).alias("listener_birth_year_share_of_track_id_percent_ra"),
        F.max("listener_birth_year").over(w_listener_id).alias("listener_id_max_listener_birth_year"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "listener_birth_year",
        "listener_birth_year_share_of_track_id",
        "prev2_listener_birth_year",
        "prev2_listener_birth_year_2",
        "listener_birth_year_vs_device_is_skipped_avg",
        "device_listener_id_min_listener_birth_year",
        "listener_birth_year_change",
        "rows_in_device_is_skipped",
        "listener_id_avg_listener_birth_year_rank",
        "listener_birth_year_share_of_track_id_dense_rank",
        "listener_birth_year_share_of_track_id_percent_ra",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        (F.col("listener_birth_year") + F.col("listener_birth_year_rank")).alias("listener_birth_year_plus_listener_birth_year_ran"),
    )
    .orderBy(F.col("track_id").desc(), "id")
    .limit(20)
)
