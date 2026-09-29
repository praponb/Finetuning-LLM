from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_played_at_hour_ties = Window.orderBy("played_at_hour")
w_is_skipped_by_played_at_hour = Window.partitionBy("is_skipped").orderBy("played_at_hour", "id").rowsBetween(-3, Window.currentRow)
w_source_by_played_at_hour = Window.partitionBy("source").orderBy("played_at_hour", "id")
w_is_skipped_by_played_at_hour_2 = Window.partitionBy("is_skipped").orderBy("played_at_hour", "id")
w_source_by_played_at_hour_run = Window.partitionBy("source").orderBy("played_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_hour_runnin = Window.orderBy("played_at_hour", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.hour(F.col("played_at")).alias("played_at_hour"),
    )
)

track_id_stats = (
    valid
    .groupBy("track_id")
    .agg(F.avg("ms_played").alias("avg_ms_played"))
)

compared = (
    valid.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.ms_played") > F.col("g.avg_ms_played"))
    .select(
        "s.*",
        "g.avg_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.avg_ms_played"), 2).alias("ms_played_minus_avg"),
    )
)

windowed = (
    compared
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_played_at_hour_ties), 3).alias("played_at_hour_percent_rank"),
        F.round(F.avg("avg_ms_played").over(w_is_skipped_by_played_at_hour), 2).alias("avg_ms_played_avg_4"),
        F.row_number().over(w_source_by_played_at_hour).alias("rn"),
        F.lead("played_at").over(w_is_skipped_by_played_at_hour_2).alias("next_played_at"),
        F.last("played_at_hour", ignorenulls=True).over(w_source_by_played_at_hour_run).alias("last_known_played_at_hour"),
        F.first("avg_ms_played", ignorenulls=True).over(w_all_by_played_at_hour_runnin).alias("first_known_avg_ms_played"),
    )
)

result = (
    windowed
    .select(
        "id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        "device_or_unknown",
        "device_slug",
        "played_at_hour",
        "avg_ms_played",
        "ms_played_minus_avg",
        "played_at_hour_percent_rank",
        "avg_ms_played_avg_4",
        "rn",
        "next_played_at",
        "last_known_played_at_hour",
        "first_known_avg_ms_played",
        F.least(F.col("avg_ms_played"), F.col("avg_ms_played_avg_4")).alias("least_avg_ms_played_avg_ms_played_avg_4"),
        (F.floor(F.col("ms_played_minus_avg") / 100000) * 100000).alias("ms_played_minus_avg_bucket"),
    )
    .orderBy(F.col("track_id").desc())
)
