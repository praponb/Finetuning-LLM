from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
    )
)

without_matches = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 13) & (~F.col("p.device").isin("tv", "car")), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["source", "device"], ["source"], ["device"]],
        "source", "device",
    )
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("source").asc_nulls_last(), F.col("device").asc_nulls_last())
)
