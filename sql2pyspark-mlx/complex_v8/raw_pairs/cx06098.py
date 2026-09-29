from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("ms_played") > 108979)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

scored = (
    scoped
    .filter(F.col("device_slug").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "device_slug",
        F.lower(F.regexp_replace(F.trim(F.col("device_slug")), " ", "_")).alias("device_slug_slug"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.hour(F.col("p.played_at")).between(8, 13)), "left_semi")
)

with_parent_listener = (
    without_matches.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.upper(F.trim(F.col("l.username"))).like("E%"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.device_slug_slug",
        F.col("l.username").alias("listener_username"),
    )
)

labeled = (
    with_parent_listener
    .select(
        "*",
        F.concat(F.col("listener_username"), F.lit("#"), F.col("id").cast("string")).alias("listener_username_ref"),
        F.concat(F.col("listener_username"), F.lit("#"), F.col("track_id").cast("string")).alias("listener_username_ref_2"),
        F.substring(F.trim(F.col("listener_username")), 1, 2).alias("listener_username_prefix"),
    )
)

per_group = (
    labeled
    .groupBy("track_id")
    .agg(
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.count(F.when(F.col("is_skipped"), 1)).alias("n_is_skipped_true_2"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
    )
)

result = (
    per_group
    .select(
        "track_id",
        "n_is_skipped_true_2",
        "n_is_skipped_false",
        "distinct_track_ids",
        (F.col("n_is_skipped_true") - F.col("n_is_skipped_true_2")).alias("n_is_skipped_true_minus_n_is_skipped_true_2"),
    )
)
