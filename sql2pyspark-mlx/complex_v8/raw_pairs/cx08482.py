from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("listener_id") <= 13)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("search", "playlist"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_counts = (
    filtered_plays.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_parent_track = (
    with_counts.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
    )
)

grouped = (
    with_parent_track
    .groupBy(
        "listener_id",
        "track_id",
        F.date_trunc("month", "played_at").alias("played_at_month"),
    )
    .agg(
        F.count(F.when(F.col("source").isin("radio", "search"), 1)).alias("n_source_in_radio_search"),
        F.count("*").alias("n_rows"),
        F.min("track_release_date").alias("first_track_release_date"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.min("source_len").alias("min_source_len"),
    )
    .filter(F.col("n_rows") >= 3)
)

tagged = grouped.withColumn("played_at_month_date", F.to_date(F.col("played_at_month")))

derived = (
    tagged
    .select(
        "listener_id",
        "track_id",
        "played_at_month",
        "min_source_len",
        (F.floor(F.col("n_is_skipped_true") / 2) * 2).alias("n_is_skipped_true_bucket"),
    )
)

filtered = (
    derived
    .crossJoin(derived.agg(F.min("min_source_len").alias("overall_min_min_source_len")))
    .filter(F.col("min_source_len") > F.col("overall_min_min_source_len"))
    .drop("overall_min_min_source_len")
)

result = (
    filtered
    .select(
        "listener_id",
        "track_id",
        "played_at_month",
        "n_is_skipped_true_bucket",
    )
    .orderBy(F.col("listener_id").desc_nulls_first())
)
