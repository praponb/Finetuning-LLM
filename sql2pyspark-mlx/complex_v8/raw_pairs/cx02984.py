from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("listener_id").orderBy("track_duration_sec", "id")

filtered_plays = (
    df_plays
    .filter(F.to_date(F.col("played_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

with_parent_listener = (
    filtered_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_label",
        "s.played_at_month",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

scored = (
    with_parent_listener
    .select(
        "*",
        F.date_format(F.col("played_at_month"), "yyyy-MM-dd HH:00").alias("played_at_month_label"),
    )
)

deduped = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

rollup_base = (
    deduped
    .groupBy("track_id", "source", "listener_id")
    .agg(
        F.sum("track_duration_sec").alias("total_track_duration_sec"),
        F.min("track_duration_sec").alias("min_track_duration_sec"),
        F.max("track_duration_sec").alias("max_track_duration_sec_check"),
    )
    .filter(F.col("max_track_duration_sec_check") > 154)
    .drop("max_track_duration_sec_check")
)

result = (
    rollup_base
    .select(
        "track_id",
        "source",
        "listener_id",
        "min_track_duration_sec",
    )
    .orderBy("source", F.col("track_id").desc_nulls_first(), "listener_id")
    .limit(10)
)
