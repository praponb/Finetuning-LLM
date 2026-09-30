from pyspark.sql import functions as F

scoped = (
    df_artists
    .filter((F.col("debut_year") < 2018) | (F.col("genre") == "Jazz"))
    .select(
        "id",
        "artist_name",
        "genre",
        "debut_year",
        F.concat_ws(": ", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
        F.when(F.col("is_verified"), 1).otherwise(0).alias("is_verified_flag"),
        F.initcap(F.trim(F.col("artist_name"))).alias("artist_name_clean"),
    )
)

with_activity = (
    scoped.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.size(F.col("t.moods")) >= 2) & (F.col("t.title").isNotNull()), "left_anti")
)

result = (
    with_activity
    .select("id", "debut_year", "is_verified_flag")
    .unpivot(
        ids=["id"],
        values=["debut_year", "is_verified_flag"],
        variableColumnName="field",
        valueColumnName="value",
    )
)
