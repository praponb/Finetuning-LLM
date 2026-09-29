from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_or_unknown_by_played = Window.partitionBy("device_or_unknown").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_or_unknown_by_played_2 = Window.partitionBy("device_or_unknown").orderBy("played_at")
w_device_or_unknown_by_played_3 = Window.partitionBy("device_or_unknown").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_or_unknown_by_played_4 = Window.partitionBy("device_or_unknown").orderBy("played_at", "id")
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .filter(F.col("source").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.nth_value("played_at", 2).over(w_device_or_unknown_by_played).alias("second_played_at"),
        F.round(F.percent_rank().over(w_device_or_unknown_by_played_2), 3).alias("played_at_percent_rank"),
        F.first("played_at", ignorenulls=True).over(w_device_or_unknown_by_played_3).alias("first_known_played_at"),
        F.lag("played_at").over(w_device_or_unknown_by_played_4).alias("prev_played_at"),
        F.last("played_at", ignorenulls=True).over(w_all_by_played_at_running).alias("last_known_played_at"),
    )
)

result = (
    windowed
    .groupBy("listener_id", "first_known_played_at")
    .pivot("source", ["playlist", "radio", "search"])
    .agg(F.sum("played_at_percent_rank"))
)
