from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("ms_played") <= 282311)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

with_activity = (
    scoped
    .crossJoin(scoped.agg(F.avg("device_len").alias("overall_avg_device_len")))
    .filter(F.col("device_len") > F.col("overall_avg_device_len"))
    .drop("overall_avg_device_len")
)

listener_id_stats = (
    with_activity
    .groupBy("listener_id")
    .agg(F.percentile_approx("device_len", 0.5).alias("median_device_len"))
)

above_average = (
    with_activity.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.device_len") >= F.col("g.median_device_len"))
    .select(
        "s.*",
        "g.median_device_len",
        F.round(F.col("s.device_len") - F.col("g.median_device_len"), 2).alias("device_len_minus_median"),
    )
)

without_matches = (
    above_average
    .crossJoin(above_average.agg(F.max("device_len_minus_median").alias("overall_max_device_len_minus_median")))
    .filter(F.col("device_len_minus_median") < F.col("overall_max_device_len_minus_median"))
    .drop("overall_max_device_len_minus_median")
)

result = (
    without_matches
    .groupingSets(
        [["listener_id", "device_len", "source"], ["listener_id"], ["device_len"], []],
        "listener_id", "device_len", "source",
    )
    .agg(
        F.grouping("listener_id").alias("is_total_listener_id"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.percentile_approx("median_device_len", 0.5).alias("median_median_device_len"),
        F.sum(F.when(F.col("source") == "album", 1).otherwise(0)).alias("n_source_album"),
    )
)
