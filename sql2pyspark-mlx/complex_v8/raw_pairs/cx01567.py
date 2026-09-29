from pyspark.sql import functions as F

base = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
        F.date_format(F.col("played_at"), "yyyy-MM-dd").alias("played_at_label"),
    )
)

screened = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 8) & (F.hour(F.col("p.played_at")).between(10, 12)), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["device", "source"], ["device"], ["source"], []],
        "device", "source",
    )
    .agg(
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
        F.round(F.stddev("ms_played"), 2).alias("stddev_ms_played"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
