from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country = Window.partitionBy("artist_country")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_tracks
    .filter(F.col("bpm") <= 147)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.country").isin("BR", "TH"))
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.is_explicit_label",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    combined_details
    .select(
        "id",
        "artist_id",
        "bpm",
        "is_explicit_label",
        "artist_country",
        "artist_debut_year",
        F.round(F.avg("artist_debut_year").over(w_artist_country), 2).alias("artist_country_avg_artist_debut_year"),
        F.first("artist_debut_year").over(w_all_by_bpm_full).alias("first_artist_debut_year"),
    )
)

result = (
    with_trends
    .rollup("artist_id", "is_explicit_label")
    .agg(
        F.round(F.avg("artist_debut_year"), 2).alias("avg_artist_debut_year"),
        F.sum(F.when(F.col("artist_country").isNotNull(), 1).otherwise(0)).alias("n_artist_country_is_not_null"),
        F.array_sort(F.collect_set("artist_country")).alias("artist_country_list"),
    )
    .select(
        "artist_id",
        F.coalesce(F.col("is_explicit_label"), F.lit("ALL")).alias("is_explicit_label"),
        "avg_artist_debut_year",
        "n_artist_country_is_not_null",
        "artist_country_list",
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit_label").asc_nulls_last(),
    )
)
