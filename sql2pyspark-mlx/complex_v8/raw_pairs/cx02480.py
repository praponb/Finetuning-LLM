from pyspark.sql import functions as F

cleaned = (
    df_artists
    .filter((F.col("debut_year").between(1991, 2018)) & (F.upper(F.trim(F.col("artist_name"))).like("P%")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

qualified = (
    cleaned.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.array_contains(F.col("t.moods"), "chill")), "left_anti")
)

qualified_2 = (
    qualified.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.duration_sec") < 353), "left_semi")
    .filter(~F.col("is_verified"))
)

filtered = (
    qualified_2.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.upper(F.trim(F.col("t.title"))).like("R%")), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
    )
)
