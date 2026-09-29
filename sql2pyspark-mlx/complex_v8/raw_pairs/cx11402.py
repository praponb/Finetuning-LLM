from pyspark.sql import functions as F

base = (
    df_plays
    .filter(F.col("ms_played") < 340114)
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("playlist", "album"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.month(F.col("played_at")).alias("played_at_month"),
    )
)

combined_details_track = (
    base.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.bpm") > 89)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

result = (
    combined_details_track
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "track_title",
        "track_bpm",
        F.lower(F.trim(F.col("track_title"))).alias("track_title_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("track_title")), " ", "_")).alias("track_title_slug"),
    )
    .orderBy("track_id", F.col("track_title").asc_nulls_last(), "id")
    .limit(10)
)
