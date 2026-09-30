from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device_by_played = Window.partitionBy("listener_id", "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(-6, -1)
w_listener_id_device_by_played_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy("ms_played_avg_6_prior", "id")
w_device_listener_id = Window.partitionBy("device", "listener_id")

valid_plays = (
    df_plays
    .filter((F.col("ms_played") > 51883) | (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

sequenced = (
    valid_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.round(F.avg("ms_played").over(w_listener_id_device_by_played), 2).alias("ms_played_avg_6_prior"),
        F.last("ms_played", ignorenulls=True).over(w_listener_id_device_by_played_2).alias("last_known_ms_played"),
    )
)

metrics = (
    sequenced
    .select(
        "*",
        F.lag("ms_played").over(w_device_is_skipped_by_ms_play).alias("prev_ms_played"),
        F.min("last_known_ms_played").over(w_device_listener_id).alias("device_listener_id_min_last_known_ms_played"),
        F.rank().over(w_device_is_skipped_by_ms_play).alias("ms_played_avg_6_prior_rank"),
        F.round(F.percent_rank().over(w_device_is_skipped_by_ms_play), 3).alias("ms_played_avg_6_prior_percent_rank"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

with_stats = (
    metrics.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at")
)

per_group = (
    with_stats
    .groupBy("listener_id", "ms_played_avg_6_prior_rank")
    .agg(
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.max("prev_ms_played").alias("max_prev_ms_played"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
    )
)

enriched = (
    per_group
    .filter(F.col("ms_played_avg_6_prior_rank") > 5)
    .select(
        "*",
        F.array_join(F.array_sort(F.col("device_list")), ",").alias("device_list_text"),
        F.when(F.col("distinct_listener_ids") >= 6, "HIGH").when(F.col("distinct_listener_ids") >= 3, "MEDIUM").otherwise("LOW").alias("distinct_listener_ids_tier"),
        F.when(F.col("distinct_listener_ids") >= 7, "HIGH").when(F.col("distinct_listener_ids") >= 5, "MEDIUM").otherwise("LOW").alias("distinct_listener_ids_tier_2"),
    )
)

with_parent_listener = (
    enriched.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.listener_id",
        "s.ms_played_avg_6_prior_rank",
        "s.distinct_listener_ids",
        "s.max_prev_ms_played",
        "s.device_list",
        "s.device_list_text",
        "s.distinct_listener_ids_tier",
        "s.distinct_listener_ids_tier_2",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

result = (
    with_parent_listener
    .select(
        "listener_id",
        "ms_played_avg_6_prior_rank",
        "max_prev_ms_played",
        "device_list_text",
        "distinct_listener_ids_tier",
        "distinct_listener_ids_tier_2",
        "listener_plan",
        "listener_interests",
    )
    .orderBy(
        F.col("distinct_listener_ids_tier").desc_nulls_first(),
        F.col("ms_played_avg_6_prior_rank").asc_nulls_last(),
    )
)
