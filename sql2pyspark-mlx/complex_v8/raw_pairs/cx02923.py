from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_debut_year_tier = Window.partitionBy("genre", "debut_year_tier")
w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified_by_debut_year = Window.partitionBy("is_verified").orderBy("debut_year", "id")

base_artists = (
    df_artists
    .filter((~F.col("country").isin("KR", "BR")) & (F.col("genre") == "HipHop"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.when(F.col("debut_year") >= 2018, "A").when(F.col("debut_year") >= 1996, "B").otherwise("C").alias("debut_year_tier"),
        F.abs(F.col("debut_year") - 1997).alias("debut_year_distance"),
    )
)

ranked = (
    base_artists
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        "debut_year_distance",
        F.round(F.avg("debut_year").over(w_genre_debut_year_tier), 2).alias("genre_debut_year_tier_avg_debut_year"),
        F.min("debut_year_distance").over(w_genre_debut_year_tier).alias("genre_debut_year_tier_min_debut_year_distance"),
        F.nth_value("debut_year_distance", 2).over(w_is_verified_by_debut_year_fu).alias("second_debut_year_distance"),
        F.dense_rank().over(w_is_verified_by_debut_year).alias("debut_year_dense_rank"),
    )
)

derived = (
    ranked
    .filter(F.col("genre_debut_year_tier_min_debut_year_distance") <= 12138)
    .select(
        "id",
        "debut_year_distance",
        "genre_debut_year_tier_avg_debut_year",
        "genre_debut_year_tier_min_debut_year_distance",
        "second_debut_year_distance",
        "debut_year_dense_rank",
        F.abs(F.col("debut_year_distance") - 809).alias("debut_year_distance_distance"),
        F.round(F.col("debut_year_distance") / F.nullif(F.col("debut_year"), F.lit(0)), 3).alias("debut_year_distance_per_debut_year"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.sum("bpm").alias("total_track_bpm"))
)

summarized_rows = (
    derived.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select("s.*", "t.total_track_bpm")
)

result = (
    summarized_rows
    .select(
        "id",
        "debut_year_distance",
        "genre_debut_year_tier_avg_debut_year",
        "genre_debut_year_tier_min_debut_year_distance",
        "second_debut_year_distance",
        "debut_year_distance_per_debut_year",
        "total_track_bpm",
        F.coalesce(F.col("total_track_bpm"), F.lit(0)).alias("total_track_bpm_filled"),
        (F.col("debut_year_distance") + F.col("total_track_bpm")).alias("debut_year_distance_plus_total_track_bpm"),
    )
)
