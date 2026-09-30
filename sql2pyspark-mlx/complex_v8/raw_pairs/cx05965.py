from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.abs(F.col("ms_played") - 160264).alias("ms_played_distance"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label_2"),
    )
)

screened = (
    scoped.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source") == "playlist"), "left_anti")
)

tagged = (
    screened
    .select(
        "*",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.concat_ws("-", F.col("played_at_label"), F.col("played_at_label_2")).alias("played_at_label_played_at_label_2_label"),
        F.concat(F.col("played_at_label"), F.lit("#"), F.col("id").cast("string")).alias("played_at_label_ref"),
    )
)

with_parent = (
    tagged.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.ms_played_distance",
        "s.is_skipped_label",
        "s.played_at_label_played_at_label_2_label",
        "s.played_at_label_ref",
        F.col("l.username").alias("listener_username"),
    )
)

with_details = (
    with_parent.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        "s.listener_username",
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
    )
)

with_parent_2 = (
    with_details.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_distance",
        "s.listener_username",
        "s.listener_birth_year",
        "s.listener_plan",
        "s.listener_interests",
        F.col("l.username").alias("listener_username_2"),
        F.col("l.birth_year").alias("listener_birth_year_2"),
        F.col("l.country").alias("listener_country"),
    )
)

result = (
    with_parent_2
    .cube("listener_id", "is_skipped")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.avg("listener_birth_year_2"), 2).alias("avg_listener_birth_year_2"),
    )
)
