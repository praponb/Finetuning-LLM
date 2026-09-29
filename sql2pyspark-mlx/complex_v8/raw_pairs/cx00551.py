from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter((F.length(F.trim(F.col("artist_name"))) >= 8) & (F.col("debut_year") > 1997))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") < 143), "left_anti")
    .filter(~F.col("is_verified"))
)

derived = (
    qualified
    .filter(F.col("debut_year").between(1994, 2018))
    .select(
        "*",
        F.length(F.trim(F.col("country"))).alias("country_len"),
        F.lower(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("release_date").alias("first_track_release_date"))
)

with_counts = (
    derived.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_track_release_date")
)

result = (
    with_counts
    .select("id", "country_len", "debut_year")
    .unpivot(
        ids=["id"],
        values=["country_len", "debut_year"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .orderBy("id", "metric")
)
