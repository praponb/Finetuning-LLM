from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter((F.col("country").isin("JP", "KR", "BR")) & (F.lower(F.col("artist_name")).like("%a %")))
    .select("id", "debut_year", F.col("country").alias("name"))
)

track_side = (
    df_tracks
    .filter((F.col("duration_sec") >= 165) & (F.length(F.trim(F.col("title"))) >= 5))
    .select(
        F.col("artist_id").alias("id"),
        F.col("id").alias("debut_year"),
        F.col("title").alias("name"),
    )
)

track_side_2 = (
    df_tracks
    .select(
        F.col("artist_id").alias("id"),
        F.col("id").alias("debut_year"),
        F.col("title").alias("name"),
    )
)

result = (
    artist_rows
    .unionByName(track_side)
    .unionByName(track_side_2)
    .orderBy(F.col("debut_year").desc(), "id")
)
