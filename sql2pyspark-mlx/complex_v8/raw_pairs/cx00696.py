from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_track_id_device_by_ms_played = Window.partitionBy("track_id", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_is_skipped), F.lit(0)), 4).alias("ms_played_share_of_is_skipped"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.nth_value("ms_played", 2).over(w_track_id_device_by_ms_played).alias("second_ms_played"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.abs(F.col("second_ms_played") - 236512).alias("second_ms_played_distance"),
    )
)

result = (
    annotated
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "ms_played_share_of_is_skipped",
        "rows_in_is_skipped",
        "second_ms_played",
        "second_ms_played_distance",
        F.abs(F.col("second_ms_played") - 195566).alias("second_ms_played_distance_2"),
    )
)
