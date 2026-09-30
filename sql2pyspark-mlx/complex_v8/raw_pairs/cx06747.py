from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source", "device").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_listener_id = Window.partitionBy("listener_id")
w_device_track_id_by_ms_played = Window.partitionBy("device", "track_id").orderBy("ms_played", "id")
w_device_track_id_by_ms_played_2 = Window.partitionBy("device", "track_id").orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_device_track_id_by_ms_played_3 = Window.partitionBy("device", "track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .filter(F.col("track_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

latest_per_key = (
    valid_plays
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived = (
    latest_per_key
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("device"), F.lit("#"), F.col("track_id").cast("string")).alias("device_ref"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.ntile(4).over(w_device_track_id_by_ms_played).alias("ms_played_quartile"),
        F.round(F.avg("ms_played").over(w_device_track_id_by_ms_played_2), 2).alias("ms_played_avg_2"),
        F.first("ms_played", ignorenulls=True).over(w_device_track_id_by_ms_played_3).alias("first_known_ms_played"),
        F.max("ms_played").over(w_device_track_id_by_ms_played_3).alias("max_ms_played_so_far"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
    )
)

result = (
    windowed
    .filter(F.col("max_ms_played_so_far") < 3201000)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source",
        "listener_id_min_ms_played",
        "ms_played_avg_2",
        "first_known_ms_played",
        "max_ms_played_so_far",
        "listener_id_sum_ms_played",
    )
    .orderBy(F.col("max_ms_played_so_far").desc())
)
