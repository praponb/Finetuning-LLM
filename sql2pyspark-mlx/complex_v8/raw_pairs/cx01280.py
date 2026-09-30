from pyspark.sql import functions as F

scoped = (
    df_tracks
    .filter((F.col("duration_sec") < 352) & (F.upper(F.trim(F.col("title"))).like("S%")))
)

track_side = (
    scoped
    .filter(F.col("is_explicit"))
    .select(
        F.lit("track").alias("source"),
        F.col("bpm").alias("ref_id"),
        F.col("release_date").alias("event_date"),
        F.col("title").alias("label"),
    )
)

track_side_2 = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        F.lit("track_2").alias("source"),
        F.col("id").alias("ref_id"),
        F.col("release_date").alias("event_date"),
        F.col("title").alias("label"),
    )
)

unioned = track_side.union(track_side_2)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("ref_id").alias("total_ref_id"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("source")
)
