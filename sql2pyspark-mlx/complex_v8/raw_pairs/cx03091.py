from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_track_id_by_ms_played_full = Window.partitionBy("track_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")
w_track_id_is_skipped_by_playe = Window.partitionBy("track_id", "is_skipped").orderBy(F.col("played_at").desc(), "id")
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")

filtered = (
    df_plays
    .filter((F.col("track_id").isNotNull()) | (F.col("device") == "phone"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

with_trends = (
    filtered
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_skipped_track_id).alias("rows_in_is_skipped_track_id"),
        F.first("played_at").over(w_track_id_by_ms_played_full).alias("first_played_at"),
        F.size(F.collect_set("listener_id").over(w_is_skipped_track_id)).alias("distinct_listener_id_in_is_skipped_track_id"),
        F.lag("ms_played", 3).over(w_track_id_by_ms_played).alias("prev3_ms_played"),
        F.ntile(5).over(w_track_id_is_skipped_by_playe).alias("played_at_quintile"),
        F.lead("ms_played").over(w_is_skipped_by_ms_played).alias("next_ms_played"),
        F.size(F.collect_set("source").over(w_is_skipped_track_id)).alias("distinct_source_in_is_skipped_track_id"),
        F.ntile(3).over(w_track_id_by_ms_played).alias("ms_played_tercile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        "rows_in_is_skipped_track_id",
        "distinct_listener_id_in_is_skipped_track_id",
        "prev3_ms_played",
        F.abs(F.col("ms_played_tercile") - 6).alias("ms_played_tercile_distance"),
        F.coalesce(F.col("next_ms_played"), F.lit(0)).alias("next_ms_played_filled"),
    )
    .orderBy("distinct_listener_id_in_is_skipped_track_id", "id")
    .limit(5)
)
