from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_distance = Window.orderBy("debut_year_distance", "id")
w_debut_year_distance_tier = Window.partitionBy("debut_year_distance_tier")

recent = (
    df_artists
    .filter(F.col("is_verified"))
    .select(
        "id",
        "genre",
        "country",
        "is_verified",
        F.abs(F.col("debut_year") - 2015).alias("debut_year_distance"),
    )
)

screened = (
    recent.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

enriched = (
    screened
    .select(
        "id",
        "is_verified",
        "debut_year_distance",
        F.when(F.col("debut_year_distance") >= 1214, "A").when(F.col("debut_year_distance") >= 809, "B").otherwise("C").alias("debut_year_distance_tier"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.concat_ws(" / ", F.col("country"), F.col("genre")).alias("country_genre_label"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "debut_year_distance",
        "country_or_unknown",
        "country_genre_label",
        F.ntile(10).over(w_all_by_debut_year_distance).alias("debut_year_distance_decile"),
        F.round(F.col("debut_year_distance") - F.avg("debut_year_distance").over(w_debut_year_distance_tier), 2).alias("debut_year_distance_vs_debut_year_distance_tier_"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["debut_year_distance_decile", "country_or_unknown"], ["debut_year_distance_decile"], ["country_or_unknown"], []],
        "debut_year_distance_decile", "country_or_unknown",
    )
    .agg(
        F.sum("debut_year_distance_decile").alias("total_debut_year_distance_decile"),
        F.count("*").alias("cnt"),
        F.round(F.sum("debut_year_distance_vs_debut_year_distance_tier_") / F.nullif(F.sum("debut_year_distance_decile"), F.lit(0)), 3).alias("debut_year_distance_vs_debut_year_distance_tier__2"),
    )
    .orderBy(
        F.col("debut_year_distance_decile").asc_nulls_last(),
        F.col("country_or_unknown").asc_nulls_last(),
    )
)
