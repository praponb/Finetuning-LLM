from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_full = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_device_by_days_since_played = Window.partitionBy("device").orderBy("days_since_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_plays
    .filter((F.to_date(F.col("played_at")) < F.lit("2025-02-06").cast("date")) & (F.col("device") != "tv"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("played_at")).alias("days_since_played_at"),
        F.concat(F.col("source"), F.lit("#"), F.col("id").cast("string")).alias("source_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

ranked = (
    cleaned
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "days_since_played_at",
        "source_ref",
        "device_slug",
        F.first("days_since_played_at").over(w_all_by_ms_played_full).alias("first_days_since_played_at"),
        F.sum("ms_played").over(w_device_by_days_since_played).alias("running_ms_played"),
    )
)

source_stats = (
    ranked
    .groupBy("source")
    .agg(F.avg("days_since_played_at").alias("avg_days_since_played_at"))
)

vs_group = (
    ranked.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.days_since_played_at") > F.col("g.avg_days_since_played_at"))
    .select(
        "s.*",
        "g.avg_days_since_played_at",
        F.round(F.col("s.days_since_played_at") - F.col("g.avg_days_since_played_at"), 2).alias("days_since_played_at_minus_avg"),
    )
)

per_group = (
    vs_group
    .filter(F.col("track_id") >= 2)
    .groupBy("device", "track_id", "listener_id")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.sum("days_since_played_at").alias("total_days_since_played_at"),
        F.max("days_since_played_at").alias("max_days_since_played_at_check"),
    )
    .filter(F.col("max_days_since_played_at_check") > 245)
    .drop("max_days_since_played_at_check")
)

result = (
    per_group
    .select(
        "device",
        "track_id",
        "listener_id",
        "device_list",
        "stddev_ms_played",
        "total_days_since_played_at",
        F.when(F.col("stddev_ms_played") >= 220000.0, "HIGH").when(F.col("stddev_ms_played") >= 140000.0, "MEDIUM").otherwise("LOW").alias("stddev_ms_played_tier"),
    )
    .orderBy(F.col("stddev_ms_played").desc_nulls_first())
)
