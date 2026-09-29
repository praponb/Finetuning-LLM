from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device = Window.partitionBy("device")
w_all_by_listener_birth_year = Window.orderBy("listener_birth_year", "id")
w_listener_plan_listener_id = Window.partitionBy("listener_plan", "listener_id")
w_track_id_by_ms_played_full = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_listener_birth_y = Window.partitionBy("track_id").orderBy(F.col("listener_birth_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan_track_id_by_ms = Window.partitionBy("listener_plan", "track_id").orderBy(F.col("ms_played").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_by_listener_birth_y_2 = Window.partitionBy("track_id").orderBy(F.col("listener_birth_year").desc())
w_listener_plan_by_device_min = Window.partitionBy("listener_plan").orderBy("device_min_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_plan = Window.partitionBy("listener_plan")

base_plays = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

joined_listener = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.username").alias("listener_username"),
    )
)

windowed = (
    joined_listener
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "listener_plan",
        "listener_username",
        F.min("ms_played").over(w_device).alias("device_min_ms_played"),
        F.round(F.percent_rank().over(w_all_by_listener_birth_year), 3).alias("listener_birth_year_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_plan_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_plan_listener_id"),
        F.first("listener_birth_year").over(w_track_id_by_ms_played_full).alias("first_listener_birth_year"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_plan_listener_id), 2).alias("ms_played_vs_listener_plan_listener_id_avg"),
        F.first("ms_played", ignorenulls=True).over(w_track_id_by_listener_birth_y).alias("first_known_ms_played"),
        F.last("ms_played", ignorenulls=True).over(w_listener_plan_track_id_by_ms).alias("last_known_ms_played"),
        F.sum("listener_birth_year").over(w_listener_plan_track_id_by_ms).alias("running_listener_birth_year"),
        F.round(F.percent_rank().over(w_track_id_by_listener_birth_y_2), 3).alias("listener_birth_year_percent_rank_2"),
    )
)

with_windows = (
    windowed
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "listener_plan",
        "listener_username",
        "device_min_ms_played",
        "listener_birth_year_percent_rank",
        "first_listener_birth_year",
        "ms_played_vs_listener_plan_listener_id_avg",
        "first_known_ms_played",
        "running_listener_birth_year",
        "listener_birth_year_percent_rank_2",
        F.first("device_min_ms_played", ignorenulls=True).over(w_listener_plan_by_device_min).alias("first_known_device_min_ms_played"),
        F.sum("last_known_ms_played").over(w_listener_plan).alias("listener_plan_sum_last_known_ms_played"),
        F.max("running_listener_birth_year").over(w_listener_plan_by_device_min).alias("max_running_listener_birth_year_so_far"),
        F.size(F.collect_set("listener_id").over(w_listener_plan)).alias("distinct_listener_id_in_listener_plan"),
        F.max("first_known_ms_played").over(w_listener_plan).alias("listener_plan_max_first_known_ms_played"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "listener_plan",
        "device_min_ms_played",
        "listener_birth_year_percent_rank",
        "first_listener_birth_year",
        "ms_played_vs_listener_plan_listener_id_avg",
        "first_known_ms_played",
        "running_listener_birth_year",
        "listener_birth_year_percent_rank_2",
        "first_known_device_min_ms_played",
        "max_running_listener_birth_year_so_far",
        "listener_plan_max_first_known_ms_played",
        F.substring(F.trim(F.col("listener_plan")), 1, 2).alias("listener_plan_prefix"),
        F.length(F.trim(F.col("listener_username"))).alias("listener_username_len"),
    )
    .orderBy(
        F.col("device_min_ms_played").asc_nulls_last(),
        F.col("first_listener_birth_year").desc(),
    )
)
