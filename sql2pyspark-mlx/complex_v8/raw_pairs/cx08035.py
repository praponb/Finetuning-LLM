from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_filled_ful = Window.orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_verified = Window.partitionBy("is_verified")
w_all_by_debut_year_filled = Window.orderBy(F.col("debut_year_filled").desc(), "id")
w_all_by_debut_year_filled_mov = Window.orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(-1, Window.currentRow)

valid = (
    df_artists
    .filter((F.col("genre").isNotNull()) & (~F.col("is_verified")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

ranked = (
    valid
    .select(
        "*",
        F.nth_value("debut_year", 2).over(w_all_by_debut_year_filled_ful).alias("second_debut_year"),
        F.round(F.col("debut_year_filled") / F.nullif(F.sum("debut_year_filled").over(w_is_verified), F.lit(0)), 4).alias("debut_year_filled_share_of_is_verified"),
        F.ntile(5).over(w_all_by_debut_year_filled).alias("debut_year_filled_quintile"),
        F.round(F.avg("debut_year_filled").over(w_all_by_debut_year_filled_mov), 2).alias("debut_year_filled_avg_2"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

without_matches = (
    filtered.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") >= 104), "left_semi")
)

scored = (
    without_matches
    .select(
        "*",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.abs(F.col("second_debut_year") - 2006).alias("second_debut_year_distance"),
        F.round(F.col("second_debut_year") / F.nullif(F.col("debut_year_filled"), F.lit(0)), 3).alias("second_debut_year_per_debut_year_filled"),
    )
)

result = (
    scored
    .filter(F.col("artist_name").isNotNull())
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "second_debut_year",
        "debut_year_filled_quintile",
        "debut_year_filled_avg_2",
        "country_or_unknown",
        "second_debut_year_per_debut_year_filled",
        F.lower(F.trim(F.col("country_or_unknown"))).alias("country_or_unknown_clean"),
    )
    .orderBy("debut_year", F.col("country_or_unknown_clean").asc_nulls_last())
)
