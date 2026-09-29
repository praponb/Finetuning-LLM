from pyspark.sql import functions as F

scoped = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("title_len").alias("title_len_v"),
        F.col("duration_sec_filled").alias("m_duration_sec_filled"),
    )
    .unpivot(
        ids=["id"],
        values=["title_len_v", "m_duration_sec_filled"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
