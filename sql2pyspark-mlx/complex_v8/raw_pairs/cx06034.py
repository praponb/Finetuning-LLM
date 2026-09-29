from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_is_skipped = Window.partitionBy("device", "is_skipped")
w_track_is_explicit_by_played = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit_by_played_2 = Window.partitionBy("track_is_explicit").orderBy(F.col("played_at").desc(), "id")
w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_is_skipped_by_ms_2 = Window.partitionBy("listener_id", "is_skipped").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc_nulls_first(), "id")
w_is_skipped_by_ms_played_ties = Window.partitionBy("is_skipped").orderBy(F.col("ms_played").desc_nulls_first())

base_plays = (
    df_plays
    .filter(F.col("ms_played") >= 56858)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

with_details = (
    base_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.signup_date") >= F.lit("2024-09-19").cast("date")), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("l.username").alias("listener_username"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.min("track_bpm").over(w_device_is_skipped).alias("device_is_skipped_min_track_bpm"),
        F.count(F.lit(1)).over(w_track_is_explicit_by_played).alias("running_count"),
        F.size(F.collect_set("listener_id").over(w_device_is_skipped)).alias("distinct_listener_id_in_device_is_skipped"),
        F.last("track_bpm", ignorenulls=True).over(w_track_is_explicit_by_played).alias("last_known_track_bpm"),
        F.row_number().over(w_track_is_explicit_by_played_2).alias("seq_no"),
        (F.col("ms_played") - F.lag("ms_played").over(w_listener_id_is_skipped_by_ms)).alias("ms_played_change"),
        F.round(F.avg("ms_played").over(w_device_is_skipped), 2).alias("device_is_skipped_avg_ms_played"),
        F.min("track_bpm").over(w_listener_id_is_skipped_by_ms_2).alias("min_track_bpm_so_far"),
        F.lag("ms_played", 2).over(w_is_skipped_by_ms_played).alias("prev2_ms_played"),
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        (F.col("ms_played") - F.lag("ms_played").over(w_is_skipped_by_ms_played)).alias("ms_played_change_2"),
        F.count(F.lit(1)).over(w_device_is_skipped).alias("rows_in_device_is_skipped"),
        F.lead("ms_played", 1, 0).over(w_listener_id_is_skipped_by_ms).alias("next_ms_played"),
    )
)

first_rows = ranked.filter((F.col("seq_no") == 1) & (F.col("last_known_track_bpm") < 142))

result = (
    first_rows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "listener_username",
        "device_is_skipped_min_track_bpm",
        "distinct_listener_id_in_device_is_skipped",
        "last_known_track_bpm",
        "seq_no",
        "min_track_bpm_so_far",
        "prev2_ms_played",
        "ms_played_change_2",
        "rows_in_device_is_skipped",
        "next_ms_played",
        F.substring(F.trim(F.col("listener_username")), 1, 2).alias("listener_username_prefix"),
        F.concat(F.col("listener_username"), F.lit("#"), F.col("track_bpm").cast("string")).alias("listener_username_ref"),
    )
    .orderBy(F.col("played_at").asc_nulls_last(), F.col("ms_played").desc(), "id")
    .limit(10)
)
