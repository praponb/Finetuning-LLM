from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter((F.col("source") == "search") & (F.col("ms_played") >= 51883))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

summary = (
    filtered_plays
    .groupBy("device", "source")
    .agg(
        F.min("ms_played").alias("min_ms_played"),
        F.sum(F.when(F.col("source") == "playlist", F.col("ms_played")).otherwise(0)).alias("ms_played_source_playlist"),
        F.sum(F.when(~F.col("source").isin("playlist", "search"), 1).otherwise(0)).alias("n_source_not_in_playlist_searc"),
        F.sum(F.when(~F.col("is_skipped"), F.col("ms_played")).otherwise(0)).alias("ms_played_not_is_skipped"),
        F.count("*").alias("row_count"),
    )
)

derived = (
    summary
    .select(
        "*",
        F.when(F.col("device").isin("car", "tv"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
    )
)

result = (
    derived
    .filter(F.col("source") == "radio")
    .select("source")
    .distinct()
    .orderBy("source")
)
