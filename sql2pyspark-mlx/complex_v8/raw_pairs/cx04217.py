from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("R%")) & (F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

with_parent_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", "a.artist_name", F.col("a.debut_year").alias("artist_debut_year"))
)

labeled = (
    with_parent_artist
    .select(
        "*",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        F.when(F.col("duration_sec") >= 392, "A").when(F.col("duration_sec") >= 164, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

result = (
    labeled
    .select(
        "id",
        "bpm",
        "duration_sec",
        "artist_debut_year",
    )
    .unpivot(
        ids=["id"],
        values=["bpm", "duration_sec", "artist_debut_year"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
