from pyspark.sql import functions as F

scoped = (
    df_artists
    .filter((F.length(F.trim(F.col("artist_name"))) >= 5) & (F.col("genre") != "Indie"))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
    )
)

artist_side = (
    scoped
    .filter((F.col("country") == "US") & (F.col("debut_year") <= 2015))
    .select(
        F.lit("artist").alias("record_type"),
        F.col("debut_year_distance").alias("entity_id"),
        F.col("id").alias("entity_id_2"),
        "country",
    )
)

track_rows = (
    df_tracks
    .filter(F.col("artist_id") <= 9)
    .select(
        F.lit("track").alias("record_type"),
        F.col("id").alias("entity_id"),
        F.col("duration_sec").alias("entity_id_2"),
        F.col("title").alias("country"),
    )
)

play_side = (
    df_plays
    .select(
        F.lit("play").alias("record_type"),
        F.col("listener_id").alias("entity_id"),
        F.col("track_id").alias("entity_id_2"),
        F.col("source").alias("country"),
    )
)

combined = (
    artist_side
    .unionByName(track_rows)
    .unionByName(play_side)
)

result = (
    combined
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id_2").alias("total_entity_id_2"),
    )
    .orderBy("record_type")
)
