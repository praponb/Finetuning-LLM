from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_is_skipped_source_by_ms_play = Window.partitionBy("is_skipped", "source").orderBy("ms_played")

base_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

metrics = (
    base_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "device_source_label",
        "source_len",
        F.size(F.collect_set("listener_id").over(w_is_skipped_track_id)).alias("distinct_listener_id_in_is_skipped_track_id"),
        F.rank().over(w_is_skipped_source_by_ms_play).alias("ms_played_rank"),
    )
)

scored = metrics.withColumn("source_prefix", F.substring(F.trim(F.col("source")), 1, 3))

result = (
    scored
    .groupBy("track_id")
    .pivot("source", ["radio", "search"])
    .agg(F.sum("source_len"))
    .orderBy("track_id")
)
