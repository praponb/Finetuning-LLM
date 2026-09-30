from pyspark.sql import functions as F

recent = (
    df_artists
    .filter((F.col("country").isin("TH", "BR")) & (F.col("debut_year").between(1991, 2018)))
    .select(
        "id",
        "artist_name",
        "country",
        "is_verified",
    )
)

filtered = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

without_matches = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
    .filter(F.col("artist_name").isNotNull())
)

tagged = (
    without_matches
    .select(
        "id",
        "artist_name",
        "is_verified",
        F.concat_ws(": ", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
    )
)

result = (
    tagged
    .filter(F.upper(F.trim(F.col("artist_name"))).like("P%"))
    .select(
        "id",
        "country_artist_name_label",
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix"),
    )
    .orderBy("id")
)
