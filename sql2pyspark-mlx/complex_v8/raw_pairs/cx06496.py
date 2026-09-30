from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("source") != "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

joined_listener = (
    base.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .filter(F.col("l.plan").isin("student", "family", "free"))
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.is_skipped_label",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("l.interests").alias("listener_interests"),
        F.col("t.moods").alias("track_moods"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.min("played_at").alias("first_played_at"))
)

with_stats = (
    joined_listener.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.first_played_at")
)

with_activity = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.played_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")) & (F.col("p.ms_played").between(95960, 359437)), "left_anti")
)

result = (
    with_activity
    .groupingSets(
        [["track_id", "is_skipped_label"], ["track_id"], []],
        "track_id", "is_skipped_label",
    )
    .agg(
        F.sum(F.when(~F.col("is_skipped"), 1).otherwise(0)).alias("n_not_is_skipped"),
        F.max("first_played_at").alias("last_first_played_at"),
    )
    .orderBy(
        F.col("track_id").asc_nulls_last(),
        F.col("is_skipped_label").asc_nulls_last(),
    )
)
