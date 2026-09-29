from pyspark.sql import functions as F

valid_plays = (
    df_plays
    .filter(F.hour(F.col("played_at")).between(9, 13))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

screened = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") < 1986) & (F.array_contains(F.col("l.interests"), "kpop")), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["listener_id", "track_id", "device"], ["listener_id"], []],
        "listener_id", "track_id", "device",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("source") == "search", 1)) / F.count("*"), 1).alias("pct_source_search"),
        F.count(F.when(F.col("device") == "phone", 1)).alias("n_device_phone"),
        F.round(F.avg("ms_played"), 2).alias("avg_ms_played"),
    )
    .orderBy(
        F.col("listener_id").asc_nulls_last(),
        F.col("track_id").asc_nulls_last(),
        F.col("device").asc_nulls_last(),
    )
)
