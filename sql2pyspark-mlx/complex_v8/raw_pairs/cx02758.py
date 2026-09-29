from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_played_at_to_date_by_la = Window.partitionBy(F.to_date("last_played_at")).orderBy(F.col("last_played_at").desc(), "ms_played_tier", "listener_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_last_played_at_to_date_by_la_2 = Window.partitionBy(F.to_date("last_played_at")).orderBy(F.col("last_played_at").desc())
w_ms_played_tier_listener_id_b = Window.partitionBy("ms_played_tier", "listener_id").orderBy("listener_birth_year", "ms_played_tier", "listener_id")
w_ms_played_tier_listener_id_b_2 = Window.partitionBy("ms_played_tier", "listener_id").orderBy("listener_birth_year")
w_ms_played_tier_listener_id = Window.partitionBy("ms_played_tier", "listener_id")

filtered_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (~F.col("device").isin("phone", "desktop")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
        F.when(F.col("ms_played") >= 282311, "HIGH").when(F.col("ms_played") >= 160264, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

enriched_listener = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device",
        "s.device_slug",
        "s.ms_played_tier",
        "s.is_skipped_flag",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
    )
)

per_group = (
    enriched_listener
    .filter(F.col("played_at") < F.lit("2025-03-02 10:00:00").cast("timestamp"))
    .groupBy("ms_played_tier", "listener_id")
    .agg(
        F.round(100 * F.count(F.when(F.col("device").isin("desktop", "car"), 1)) / F.count("*"), 1).alias("pct_device_in_desktop_car"),
        F.max("is_skipped_flag").alias("max_is_skipped_flag"),
    )
)

joined = (
    per_group.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") >= 1968), "inner")
    .select(
        "s.ms_played_tier",
        "s.listener_id",
        "s.pct_device_in_desktop_car",
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    joined.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at")
)

ranked = (
    with_counts
    .select(
        "ms_played_tier",
        "listener_id",
        "pct_device_in_desktop_car",
        "last_played_at",
        F.sum("listener_birth_year").over(w_last_played_at_to_date_by_la).alias("running_listener_birth_year"),
        F.rank().over(w_last_played_at_to_date_by_la_2).alias("last_played_at_rank"),
        F.row_number().over(w_ms_played_tier_listener_id_b).alias("rn"),
        F.round(F.percent_rank().over(w_ms_played_tier_listener_id_b_2), 3).alias("listener_birth_year_percent_rank"),
        F.round(F.avg("listener_birth_year").over(w_ms_played_tier_listener_id), 2).alias("ms_played_tier_listener_id_avg_listener_birth_ye"),
    )
)

result = (
    ranked
    .groupBy("last_played_at")
    .pivot("ms_played_tier", ["HIGH", "MEDIUM", "LOW"])
    .agg(F.count("ms_played_tier_listener_id_avg_listener_birth_ye"))
    .orderBy("last_played_at")
)
