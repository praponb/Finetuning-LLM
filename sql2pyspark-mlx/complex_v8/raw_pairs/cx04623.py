from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified = Window.partitionBy("is_verified")
w_is_verified_by_debut_year_ti = Window.partitionBy("is_verified").orderBy("debut_year")
w_is_verified_by_debut_year_ru = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_verified_by_debut_year = Window.partitionBy("is_verified").orderBy("debut_year", "id")

scoped = (
    df_artists
    .filter((F.col("debut_year").between(1994, 2015)) | (F.upper(F.trim(F.col("artist_name"))).like("S%")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.substring(F.trim(F.col("genre")), 1, 2).alias("genre_prefix"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_is_verified), 2).alias("debut_year_vs_is_verified_avg"),
        F.dense_rank().over(w_is_verified_by_debut_year_ti).alias("debut_year_dense_rank"),
        F.sum("debut_year").over(w_is_verified_by_debut_year_ru).alias("running_debut_year"),
        F.lag("debut_year").over(w_is_verified_by_debut_year).alias("prev_debut_year"),
    )
)

labeled = (
    metrics
    .filter(F.col("running_debut_year") < 14758)
    .select(
        "*",
        F.coalesce(F.col("prev_debut_year"), F.lit(0)).alias("prev_debut_year_filled"),
        F.round(F.col("prev_debut_year") / F.nullif(F.col("debut_year"), F.lit(0)), 3).alias("prev_debut_year_per_debut_year"),
    )
)

without_matches = (
    labeled.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left_semi")
)

country_stats = (
    without_matches
    .groupBy("country")
    .agg(F.percentile_approx("running_debut_year", 0.5).alias("median_running_debut_year"))
)

compared = (
    without_matches.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.running_debut_year") >= F.col("g.median_running_debut_year"))
    .select(
        "s.*",
        "g.median_running_debut_year",
        F.round(F.col("s.running_debut_year") - F.col("g.median_running_debut_year"), 2).alias("running_debut_year_minus_median"),
    )
)

scored = (
    compared
    .filter(F.col("running_debut_year_minus_median") < 8092.0)
    .select(
        "*",
        F.greatest(F.col("debut_year"), F.col("prev_debut_year_filled")).alias("greatest_debut_year_prev_debut_year_filled"),
    )
)

result = (
    scored
    .select(
        "id",
        "artist_name",
        "debut_year",
        "is_verified",
        "debut_year_dense_rank",
        "prev_debut_year",
        "prev_debut_year_per_debut_year",
        "running_debut_year_minus_median",
        "greatest_debut_year_prev_debut_year_filled",
        F.abs(F.col("prev_debut_year_filled") - 2003).alias("prev_debut_year_filled_distance"),
        F.concat_ws(" / ", F.col("genre_prefix"), F.col("country")).alias("genre_prefix_country_label"),
    )
    .orderBy(
        F.col("running_debut_year_minus_median").desc(),
        F.col("prev_debut_year_per_debut_year").asc(),
    )
)
