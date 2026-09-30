from pyspark.sql import functions as F

valid = (
    df_plays
    .filter(F.col("device") == "phone")
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.concat_ws("-", F.col("device"), F.col("source")).alias("device_source_label"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

annotated = (
    valid
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "device_source_label",
        F.lower(F.regexp_replace(F.trim(F.col("device_source_label")), " ", "_")).alias("device_source_label_slug"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

summarized_rows = (
    annotated.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

listener_id_stats = (
    summarized_rows
    .groupBy("listener_id")
    .agg(F.percentile_approx("play_count", 0.5).alias("median_play_count"))
)

vs_group = (
    summarized_rows.alias("s")
    .join(listener_id_stats.alias("g"), F.col("g.listener_id") == F.col("s.listener_id"), "inner")
    .filter(F.col("s.play_count") >= F.col("g.median_play_count"))
    .select(
        "s.*",
        "g.median_play_count",
        F.round(F.col("s.play_count") - F.col("g.median_play_count"), 2).alias("play_count_minus_median"),
    )
)

scored = (
    vs_group
    .select(
        "*",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.coalesce(F.col("play_count_minus_median"), F.lit(0.0)).alias("play_count_minus_median_filled"),
    )
)

filtered = (
    scored
    .crossJoin(scored.agg(F.max("play_is_skipped_count").alias("overall_max_play_is_skipped_count")))
    .filter(F.col("play_is_skipped_count") < F.col("overall_max_play_is_skipped_count"))
    .drop("overall_max_play_is_skipped_count")
)

stats = (
    filtered
    .groupBy("is_skipped_label", "play_count", "is_skipped")
    .agg(
        F.min("first_played_at").alias("first_first_played_at"),
        F.round(F.avg("play_count_minus_median"), 2).alias("avg_play_count_minus_median"),
        F.round(F.sum("play_count_minus_median_filled") / F.nullif(F.sum("play_count_minus_median"), F.lit(0)), 3).alias("play_count_minus_median_filled_to_play_count_min"),
        F.max("play_is_skipped_count").alias("max_play_is_skipped_count_check"),
    )
    .filter(F.col("max_play_is_skipped_count_check") > 2)
    .drop("max_play_is_skipped_count_check")
)

result = (
    stats
    .select(
        "is_skipped_label",
        "play_count",
        "is_skipped",
        "first_first_played_at",
        "avg_play_count_minus_median",
        "play_count_minus_median_filled_to_play_count_min",
        F.length(F.trim(F.col("is_skipped_label"))).alias("is_skipped_label_len"),
    )
)
