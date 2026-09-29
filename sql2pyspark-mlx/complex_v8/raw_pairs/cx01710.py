from pyspark.sql import functions as F

scoped = (
    df_plays
    .filter(F.col("track_id") >= 16)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

play_rows = (
    scoped
    .filter((~F.col("is_skipped")) & (F.col("played_at_label").isNotNull()))
    .select(
        F.lit("play").alias("source"),
        F.col("ms_played").alias("ref_id"),
        F.col("played_at").alias("event_time"),
    )
)

play_side = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        F.lit("play_2").alias("source"),
        F.col("listener_id").alias("ref_id"),
        F.col("played_at").alias("event_time"),
    )
)

stacked = (
    play_rows
    .unionByName(play_side)
    .distinct()
)

result = (
    stacked
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
