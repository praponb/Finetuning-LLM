from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_by_debut_year_fu = Window.partitionBy("is_verified").orderBy("debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre = Window.partitionBy("genre")
w_country_by_debut_year = Window.partitionBy("country").orderBy("debut_year", "id")
w_country_by_debut_year_ties = Window.partitionBy("country").orderBy("debut_year")

scoped = (
    df_artists
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
    )
)

ranked = (
    scoped
    .select(
        "id",
        "genre",
        "debut_year",
        "is_verified",
        F.first("debut_year").over(w_is_verified_by_debut_year_fu).alias("first_debut_year"),
        F.count(F.lit(1)).over(w_genre).alias("rows_in_genre"),
        F.ntile(3).over(w_country_by_debut_year).alias("debut_year_tercile"),
        F.round(F.cume_dist().over(w_country_by_debut_year_ties), 3).alias("debut_year_cume_dist"),
        F.rank().over(w_country_by_debut_year_ties).alias("debut_year_rank"),
    )
)

enriched = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("genre"))).alias("genre_len"),
        F.round(F.col("debut_year_cume_dist") / F.nullif(F.col("debut_year_tercile"), F.lit(0)), 3).alias("debut_year_cume_dist_per_debut_year_tercile"),
        F.lower(F.regexp_replace(F.trim(F.col("genre")), " ", "_")).alias("genre_slug"),
    )
)

tagged = (
    enriched
    .filter(F.col("rows_in_genre").between(6, 19))
    .select("*", F.lower(F.trim(F.col("genre_slug"))).alias("genre_slug_clean"))
)

annotated = (
    tagged
    .select(
        "*",
        F.abs(F.col("debut_year_tercile") - 4).alias("debut_year_tercile_distance"),
        F.round(F.col("debut_year") / F.nullif(F.col("debut_year_cume_dist"), F.lit(0)), 3).alias("debut_year_per_debut_year_cume_dist"),
    )
)

with_activity = (
    annotated.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (~F.col("t.is_explicit")) & (F.upper(F.trim(F.col("t.title"))).like("R%")), "left_semi")
)

genre_stats = (
    with_activity
    .groupBy("genre")
    .agg(F.percentile_approx("first_debut_year", 0.5).alias("median_first_debut_year"))
)

above_average = (
    with_activity.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.first_debut_year") >= F.col("g.median_first_debut_year"))
    .select(
        "s.*",
        "g.median_first_debut_year",
        F.round(F.col("s.first_debut_year") - F.col("g.median_first_debut_year"), 2).alias("first_debut_year_minus_median"),
    )
)

result = (
    above_average
    .groupBy("genre_len", "debut_year_tercile")
    .pivot("genre", ["Indie", "Classical", "Jazz"])
    .agg(F.max("debut_year_rank"))
    .orderBy("genre_len", "debut_year_tercile")
)
