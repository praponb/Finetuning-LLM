from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("duration_sec") >= 165)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

annotated = (
    prepared
    .filter(F.col("is_explicit_label") == "no")
    .select(
        "*",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

result = (
    annotated
    .select(
        "id",
        F.col("duration_sec").alias("m_duration_sec"),
        F.col("title_len").alias("title_len_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_duration_sec", "title_len_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
