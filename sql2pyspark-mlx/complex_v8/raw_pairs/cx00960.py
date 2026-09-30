from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter((F.col("bpm") >= 104) & (F.lower(F.col("title")).like("%pe%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

track_side = (
    recent
    .filter((F.length(F.trim(F.col("title"))) >= 8) & (F.col("release_date").between(F.lit("2024-04-21").cast("date"), F.lit("2025-04-02").cast("date"))))
    .select("title", "release_date", F.lit("track").alias("record_type"))
)

listener_side = df_listeners.select("username", "signup_date", F.lit("listener").alias("record_type"))

stacked = track_side.union(listener_side)

result = (
    stacked
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("release_date").alias("latest_release_date"),
    )
    .orderBy("record_type")
)
