from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_is_verified = Window.partitionBy("genre", "is_verified")
w_country_by_debut_year_filled = Window.partitionBy("country").orderBy(F.col("debut_year_filled_minus_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_artists = df_artists.filter(F.col("debut_year") < 2016)

without_matches = (
    scoped_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.upper(F.trim(F.col("t.title"))).like("S%")), "left_semi")
)

scored = (
    without_matches
    .filter(F.col("country").isNotNull())
    .select(
        "*",
        F.concat_ws(": ", F.col("country"), F.col("genre")).alias("country_genre_label"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
    )
)

without_matches_2 = (
    scored.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

country_stats = (
    without_matches_2
    .groupBy("country")
    .agg(F.avg("debut_year_filled").alias("avg_debut_year_filled"))
)

vs_group = (
    without_matches_2.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.debut_year_filled") > F.col("g.avg_debut_year_filled"))
    .select(
        "s.*",
        "g.avg_debut_year_filled",
        F.round(F.col("s.debut_year_filled") - F.col("g.avg_debut_year_filled"), 2).alias("debut_year_filled_minus_avg"),
    )
)

with_windows = (
    vs_group
    .withColumn("avg_debut_year_filled_share_of_genre_is_verified", F.round(F.col("avg_debut_year_filled") / F.nullif(F.sum("avg_debut_year_filled").over(w_genre_is_verified), F.lit(0)), 4))
    .withColumn("second_debut_year_filled_minus_avg", F.nth_value("debut_year_filled_minus_avg", 2).over(w_country_by_debut_year_filled))
)

stats = (
    with_windows
    .filter(F.upper(F.trim(F.col("artist_name"))).like("L%"))
    .groupBy("country", "is_verified", "genre")
    .agg(
        F.round(F.sum("avg_debut_year_filled") / F.nullif(F.sum("debut_year_filled"), F.lit(0)), 3).alias("avg_debut_year_filled_to_debut_year_filled_ratio"),
        F.round(F.sum("avg_debut_year_filled_share_of_genre_is_verified") / F.nullif(F.sum("debut_year_filled_minus_avg"), F.lit(0)), 3).alias("avg_debut_year_filled_share_of_genre_is_verified_2"),
        F.max("second_debut_year_filled_minus_avg").alias("max_second_debut_year_filled_minus_avg"),
        F.sum(F.when(F.col("genre") != "Jazz", F.col("debut_year_filled"))).alias("debut_year_filled_genre_jazz"),
        F.sum(F.when(F.col("country") != "TH", F.col("avg_debut_year_filled")).otherwise(0)).alias("avg_debut_year_filled_country_th"),
        F.countDistinct("genre").alias("distinct_genres"),
        F.sum("second_debut_year_filled_minus_avg").alias("sum_second_debut_year_filled_minus_avg_check"),
    )
    .filter(F.col("sum_second_debut_year_filled_minus_avg_check") > -606.9)
    .drop("sum_second_debut_year_filled_minus_avg_check")
)

result = (
    stats
    .groupBy("distinct_genres")
    .pivot("genre", ["Pop", "Classical", "HipHop", "Indie"])
    .agg(F.count("avg_debut_year_filled_to_debut_year_filled_ratio"))
)
