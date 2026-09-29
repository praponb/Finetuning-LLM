from pyspark.sql import functions as F

filtered_artists = (
    df_artists
    .filter((F.lower(F.col("artist_name")).like("%ht%")) & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.concat_ws(" / ", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
    )
)

artist_rows = (
    filtered_artists
    .filter((~F.col("is_verified")) & (F.col("genre") != "Indie"))
    .select("debut_year", F.col("genre").alias("category"))
)

play_rows = (
    df_plays
    .filter((F.col("listener_id") >= 3) & (F.col("source") == "playlist"))
    .select(
        F.col("track_id").alias("debut_year"),
        F.col("device").alias("category"),
    )
)

result = artist_rows.exceptAll(play_rows)
