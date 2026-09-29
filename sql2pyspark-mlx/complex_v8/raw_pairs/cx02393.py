from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

filtered = (
    filtered_tracks.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.title").isNotNull()), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        F.col("bpm").alias("bpm_v"),
        F.col("duration_sec_minus_bpm").alias("duration_sec_minus_bpm_v"),
        F.col("duration_sec").alias("m_duration_sec"),
    )
    .unpivot(
        ids=["id"],
        values=["bpm_v", "duration_sec_minus_bpm_v", "m_duration_sec"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
