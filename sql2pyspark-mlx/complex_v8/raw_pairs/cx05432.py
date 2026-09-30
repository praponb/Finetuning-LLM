from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")

cleaned = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "workout"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
    )
)

enriched_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Pop", "Jazz")), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

artist_id_stats = (
    enriched_artist
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

compared = (
    enriched_artist.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

with_windows = (
    compared
    .withColumn("artist_id_min_max_bpm", F.min("max_bpm").over(w_artist_id))
    .withColumn("max_bpm_vs_artist_id_avg", F.round(F.col("max_bpm") - F.avg("max_bpm").over(w_artist_id), 2))
)

combined_details = (
    with_windows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.artist_is_verified",
        "s.artist_name",
        "s.artist_debut_year",
        "s.max_bpm",
        "s.bpm_minus_max",
        "s.artist_id_min_max_bpm",
        "s.max_bpm_vs_artist_id_avg",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "artist_is_verified",
        "artist_name",
        "artist_debut_year",
        "max_bpm",
        "bpm_minus_max",
        "artist_id_min_max_bpm",
        "max_bpm_vs_artist_id_avg",
        "artist_country",
        "artist_genre",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
    .orderBy(F.col("max_bpm_vs_artist_id_avg").desc(), F.col("artist_name").desc())
)
