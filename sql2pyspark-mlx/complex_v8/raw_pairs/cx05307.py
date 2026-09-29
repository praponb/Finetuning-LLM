from pyspark.sql import functions as F

artist_rows = (
    df_artists
    .filter(F.col("genre") != "HipHop")
    .select(F.lit("artist").alias("origin"), "debut_year", "id")
)

listener_side = (
    df_listeners
    .filter(F.col("country") == "UK")
    .select(
        F.lit("listener").alias("origin"),
        F.col("birth_year").alias("debut_year"),
        "id",
    )
)

combined = (
    artist_rows
    .unionByName(listener_side)
    .distinct()
)

result = (
    combined
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("debut_year").alias("total_debut_year"),
    )
    .orderBy("origin")
)
