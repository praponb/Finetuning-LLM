from pyspark.sql import functions as F

cleaned_plays = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

summarized_rows = (
    cleaned_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at")
)

annotated = summarized_rows.withColumn("ms_played_distance", F.abs(F.col("ms_played") - 183484))

result = (
    annotated
    .cube("source", "listener_id", "track_id")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
    )
    .select(
        F.coalesce(F.col("source"), F.lit("ALL")).alias("source"),
        "listener_id",
        "track_id",
        "row_count",
        "source_list",
    )
)
