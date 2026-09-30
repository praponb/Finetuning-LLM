from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_track_id_by_ms_played_ties = Window.partitionBy("track_id").orderBy(F.col("ms_played").asc_nulls_last())

filtered_plays = (
    df_plays
    .filter((F.col("source").isNotNull()) & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

screened = (
    filtered_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.device").isin("tv", "car")), "left_semi")
)

windowed = (
    screened
    .select(
        "id",
        "played_at",
        "ms_played",
        "played_at_label",
        F.size(F.collect_set("device").over(w_track_id)).alias("distinct_device_in_track_id"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id), F.lit(0)), 4).alias("ms_played_share_of_track_id"),
        F.round(F.percent_rank().over(w_track_id_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
    )
)

screened_2 = (
    windowed
    .crossJoin(windowed.agg(F.avg("distinct_device_in_track_id").alias("overall_avg_distinct_device_in_track_id")))
    .filter(F.col("distinct_device_in_track_id") > F.col("overall_avg_distinct_device_in_track_id"))
    .drop("overall_avg_distinct_device_in_track_id")
)

tagged = (
    screened_2
    .filter(F.col("ms_played_percent_rank") > 0.4)
    .select(
        "*",
        F.abs(F.col("ms_played") - 221269).alias("ms_played_distance"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.substring(F.trim(F.col("played_at_label")), 1, 4).alias("played_at_label_prefix"),
    )
)

rollup_base = (
    tagged
    .groupBy(
        F.date_trunc("month", "played_at").alias("played_at_month"),
        "distinct_device_in_track_id",
    )
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.min("ms_played_distance").alias("min_ms_played_distance"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("distinct_device_in_track_id").alias("sum_distinct_device_in_track_id_check"),
    )
    .filter(F.col("sum_distinct_device_in_track_id_check") > 4)
    .drop("sum_distinct_device_in_track_id_check")
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.max("min_ms_played_distance").alias("overall_max_min_ms_played_distance")))
    .filter(F.col("min_ms_played_distance") < F.col("overall_max_min_ms_played_distance"))
    .drop("overall_max_min_ms_played_distance")
)

result = (
    with_activity
    .filter(F.col("distinct_ids") < 8)
    .select(
        "played_at_month",
        "distinct_device_in_track_id",
        "last_played_at",
        "min_ms_played_distance",
        "distinct_ids",
    )
    .orderBy("played_at_month", "distinct_device_in_track_id")
    .limit(20)
)
