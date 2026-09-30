from pyspark.sql import functions as F

scoped = (
    df_artists
    .filter((F.col("genre") == "Pop") | (F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

artist_rows = (
    scoped
    .filter((F.col("debut_year") <= 2020) & (~F.col("is_verified")))
    .select("country_slug")
)

track_side = (
    df_tracks
    .filter(F.col("duration_sec") >= 213)
    .select("title")
)

result = (
    artist_rows
    .subtract(track_side)
    .orderBy(F.col("country_slug").desc())
)
