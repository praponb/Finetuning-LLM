from pyspark.sql import functions as F

scoped_artists = (
    df_artists
    .filter((F.col("country").isin("US", "JP", "TH")) & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.substring(F.trim(F.col("artist_name")), 1, 3).alias("artist_name_prefix"),
    )
)

artist_rows = scoped_artists.select("country", F.col("genre").alias("category"))

artist_side = (
    df_artists
    .filter((F.col("is_verified")) & (F.col("country").isNotNull()))
    .select("artist_name", "genre")
)

play_side = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("ms_played") <= 324058))
    .select("device", "source")
)

all_events = (
    artist_rows
    .union(artist_side)
    .union(play_side)
)

result = (
    all_events
    .groupBy("country")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("country")
)
