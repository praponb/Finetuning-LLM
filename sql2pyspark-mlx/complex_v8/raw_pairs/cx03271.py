from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc(), "id")
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy(F.col("played_at").desc())
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_device_listener_id_by_played = Window.partitionBy("device", "listener_id").orderBy("played_at", "id")
w_device_listener_id_by_played_2 = Window.partitionBy("device", "listener_id").orderBy("played_at")

scoped_plays = (
    df_plays
    .filter((F.col("device").isin("car", "speaker")) | (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

windowed = (
    scoped_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at"),
        F.lead("played_at").over(w_listener_id_by_played_at).alias("next_played_at"),
        F.dense_rank().over(w_listener_id_by_played_at_tie).alias("played_at_dense_rank"),
        F.lag("played_at", 2).over(w_all_by_played_at).alias("prev2_played_at"),
        F.ntile(10).over(w_listener_id_by_played_at).alias("played_at_decile"),
        F.lag("played_at").over(w_device_listener_id_by_played).alias("prev_played_at"),
        F.rank().over(w_device_listener_id_by_played_2).alias("played_at_rank"),
        F.round(F.cume_dist().over(w_device_listener_id_by_played), 3).alias("played_at_cume_dist"),
    )
)

labeled = (
    windowed
    .select(
        "id",
        "track_id",
        "source",
        "second_played_at",
        "next_played_at",
        "played_at_dense_rank",
        "played_at_decile",
        "prev_played_at",
        "played_at_rank",
        "played_at_cume_dist",
        F.round((F.unix_timestamp(F.col("second_played_at")) - F.unix_timestamp(F.col("prev2_played_at"))) / 3600, 1).alias("hours_prev2_played_at_to_second_played_at"),
        F.concat(F.col("source"), F.lit("#"), F.col("played_at_decile").cast("string")).alias("source_ref"),
        (F.floor(F.col("played_at_rank") / 2) * 2).alias("played_at_rank_bucket"),
    )
)

result = (
    labeled
    .select(
        "id",
        "track_id",
        "source",
        "next_played_at",
        "played_at_dense_rank",
        "played_at_decile",
        "prev_played_at",
        "played_at_cume_dist",
        "hours_prev2_played_at_to_second_played_at",
        "source_ref",
        "played_at_rank_bucket",
        F.concat(F.col("source"), F.lit("#"), F.col("track_id").cast("string")).alias("source_ref_2"),
    )
    .orderBy(F.col("next_played_at").asc())
)
