from pyspark.sql import functions as F

valid = (
    df_artists
    .filter(F.col("debut_year").between(1994, 2016))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        F.concat(F.col("artist_name"), F.lit("#"), F.col("id").cast("string")).alias("artist_name_ref"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

with_children_tracks = (
    valid.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.country_or_unknown",
        F.col("t.id").alias("track_id"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.release_date").alias("track_release_date"),
    )
)

enriched = (
    with_children_tracks
    .filter(~F.col("country_or_unknown").isin("TH", "UK"))
    .withColumn("artist_name_prefix", F.substring(F.trim(F.col("artist_name")), 1, 3))
)

result = (
    enriched
    .groupingSets(
        [["track_is_explicit", "country_or_unknown"], ["track_is_explicit"]],
        "track_is_explicit", "country_or_unknown",
    )
    .agg(F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"))
    .select(
        "track_is_explicit",
        F.coalesce(F.col("country_or_unknown"), F.lit("ALL")).alias("country_or_unknown"),
        "artist_name_list",
    )
)
