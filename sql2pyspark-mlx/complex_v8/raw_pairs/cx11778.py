from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_played_at = Window.orderBy(F.col("days_since_played_at").desc(), "id").rowsBetween(-6, -1)
w_all_by_days_since_played_at_2 = Window.orderBy(F.col("days_since_played_at").desc(), "id")
w_source_len_device = Window.partitionBy("source_len", "device")

prepared = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")) & (F.col("device").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("played_at")).alias("days_since_played_at"),
    )
)

ranked = (
    prepared
    .select(
        "*",
        F.max("source_len").over(w_all_by_days_since_played_at).alias("source_len_max_6_prior"),
        F.ntile(3).over(w_all_by_days_since_played_at_2).alias("days_since_played_at_tercile"),
        F.sum("ms_played").over(w_source_len_device).alias("source_len_device_sum_ms_played"),
        F.lag("ms_played").over(w_all_by_days_since_played_at_2).alias("prev_ms_played"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "days_since_played_at_tercile",
        "prev_ms_played",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.concat(F.col("device"), F.lit("#"), F.col("prev_ms_played").cast("string")).alias("device_ref"),
    )
)

with_parent_listener = (
    annotated.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device_prefix",
        F.col("l.country").alias("listener_country"),
    )
)

tagged = (
    with_parent_listener
    .select(
        "*",
        F.upper(F.trim(F.col("listener_country"))).alias("listener_country_clean"),
        F.length(F.trim(F.col("listener_country"))).alias("listener_country_len"),
        F.substring(F.trim(F.col("device_prefix")), 1, 2).alias("device_prefix_prefix"),
    )
)

result = (
    tagged
    .filter(F.col("listener_country_len") >= 6)
    .select(
        "id",
        "listener_id",
        "device_prefix",
        "listener_country_clean",
        "device_prefix_prefix",
    )
    .orderBy(F.col("id").desc())
)
