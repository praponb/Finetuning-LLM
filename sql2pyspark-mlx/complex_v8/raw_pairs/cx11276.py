from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played")
w_played_at_to_date_device_by = Window.partitionBy(F.to_date("played_at"), "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_to_date_device_by_2 = Window.partitionBy(F.to_date("played_at"), "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, Window.currentRow)

prepared_plays = (
    df_plays
    .filter(~F.col("device").isin("phone", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("ms_played").cast("string")).alias("source_ref"),
    )
)

with_trends = (
    prepared_plays
    .select(
        "*",
        F.size(F.collect_set("device").over(w_is_skipped)).alias("distinct_device_in_is_skipped"),
        F.round(F.percent_rank().over(w_source_listener_id_by_ms_pla), 3).alias("ms_played_percent_rank"),
        F.sum("ms_played").over(w_played_at_to_date_device_by).alias("running_ms_played"),
        F.min("ms_played").over(w_is_skipped).alias("is_skipped_min_ms_played"),
        F.round(F.avg("ms_played").over(w_played_at_to_date_device_by_2), 2).alias("ms_played_avg_7"),
        F.first("played_at", ignorenulls=True).over(w_played_at_to_date_device_by).alias("first_known_played_at"),
    )
)

joined_listener = (
    with_trends.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.*",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    joined_listener
    .groupingSets(
        [["listener_id", "listener_plan", "source"], ["listener_id"], ["listener_plan"]],
        "listener_id", "listener_plan", "source",
    )
    .agg(
        F.round(F.stddev("distinct_device_in_is_skipped"), 2).alias("stddev_distinct_device_in_is_skipped"),
        F.round(F.avg("is_skipped_min_ms_played"), 2).alias("avg_is_skipped_min_ms_played"),
        F.min("first_known_played_at").alias("first_first_known_played_at"),
    )
)
