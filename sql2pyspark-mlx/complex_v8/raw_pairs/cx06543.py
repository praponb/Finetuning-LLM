from pyspark.sql import functions as F

prepared_artists = (
    df_artists
    .filter((F.col("country").isin("US", "BR", "KR")) & (F.col("debut_year") < 2015))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
    )
)

artist_rows = (
    prepared_artists
    .select(
        F.col("genre").alias("label"),
        F.col("artist_name").alias("label_2"),
        "id",
    )
)

artist_side = df_artists.select("artist_name", "genre", "debut_year")

play_side = (
    df_plays
    .filter(F.col("ms_played").between(97825, 301865))
    .select("device", "source", "track_id")
)

result = (
    artist_rows
    .union(artist_side)
    .union(play_side)
    .orderBy("label_2", "label")
)
