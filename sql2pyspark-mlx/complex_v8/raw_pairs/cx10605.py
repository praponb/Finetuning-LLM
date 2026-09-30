from pyspark.sql import functions as F

filtered_plays = (
    df_plays
    .filter(F.col("listener_id") <= 8)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
    )
)

enriched_track = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.source",
        F.col("t.moods").alias("track_moods"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

enriched = (
    enriched_track
    .filter(F.size(F.col("track_moods")) >= 1)
    .select(
        "*",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

summary = (
    enriched
    .groupBy("source", "track_id")
    .agg(
        F.sum(F.when(F.col("device") == "car", 1).otherwise(0)).alias("n_device_car"),
        F.count(F.when(F.col("source") != "radio", 1)).alias("n_source_radio"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    summary
    .select("source", "n_rows")
    .distinct()
    .orderBy(F.col("source").desc())
)
