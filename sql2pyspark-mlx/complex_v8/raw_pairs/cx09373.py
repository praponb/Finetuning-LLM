from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_debut_year_prior_1 = Window.orderBy("debut_year", "id").rowsBetween(-1, -1)
w_is_verified_country = Window.partitionBy("is_verified", "country")
w_genre_by_is_verified_country = Window.partitionBy("genre").orderBy(F.col("is_verified_country_avg_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_genre_by_is_verified_country_2 = Window.partitionBy("genre").orderBy(F.col("is_verified_country_avg_debut_year").desc(), "id")
w_genre_by_is_verified_country_3 = Window.partitionBy("genre").orderBy(F.col("is_verified_country_avg_debut_year").desc())

filtered = (
    df_artists
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.initcap(F.trim(F.col("genre"))).alias("genre_clean"),
        F.concat_ws("|", F.col("genre"), F.col("country")).alias("genre_country_label"),
    )
)

metrics = (
    filtered
    .withColumn("debut_year_avg_1_prior", F.round(F.avg("debut_year").over(w_all_by_debut_year_prior_1), 2))
    .withColumn("is_verified_country_avg_debut_year", F.round(F.avg("debut_year").over(w_is_verified_country), 2))
)

genre_stats = (
    metrics
    .groupBy("genre")
    .agg(F.avg("debut_year").alias("avg_debut_year"))
)

above_average = (
    metrics.alias("s")
    .join(genre_stats.alias("g"), F.col("g.genre") == F.col("s.genre"), "inner")
    .filter(F.col("s.debut_year") > F.col("g.avg_debut_year"))
    .select(
        "s.*",
        "g.avg_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.avg_debut_year"), 2).alias("debut_year_minus_avg"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.bpm") > 100) & (F.col("t.duration_sec") <= 344), "left_anti")
    .filter(F.lower(F.col("genre_clean")).like("%az%"))
)

sequenced = (
    without_matches
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        "genre_clean",
        "genre_country_label",
        "debut_year_avg_1_prior",
        "is_verified_country_avg_debut_year",
        "avg_debut_year",
        "debut_year_minus_avg",
        F.first("debut_year_avg_1_prior", ignorenulls=True).over(w_genre_by_is_verified_country).alias("first_known_debut_year_avg_1_prior"),
        F.ntile(3).over(w_genre_by_is_verified_country_2).alias("is_verified_country_avg_debut_year_tercile"),
        F.max("debut_year_minus_avg").over(w_genre_by_is_verified_country).alias("max_debut_year_minus_avg_so_far"),
        F.round(F.percent_rank().over(w_genre_by_is_verified_country_3), 3).alias("is_verified_country_avg_debut_year_percent_rank"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

summarized_rows = (
    sequenced.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

screened = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.release_date").isNotNull()), "left_anti")
    .filter(F.col("track_count").between(0, 6))
)

result = (
    screened
    .filter(F.col("debut_year_minus_avg") < 404.6)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "genre_clean",
        "genre_country_label",
        "debut_year_avg_1_prior",
        "is_verified_country_avg_debut_year",
        "avg_debut_year",
        "debut_year_minus_avg",
        "is_verified_country_avg_debut_year_tercile",
        "max_debut_year_minus_avg_so_far",
        "is_verified_country_avg_debut_year_percent_rank",
        "track_is_explicit_count",
        (F.col("track_is_explicit_count") + F.col("debut_year_avg_1_prior")).alias("track_is_explicit_count_plus_debut_year_avg_1_pr"),
    )
    .orderBy(
        F.col("debut_year_minus_avg").asc_nulls_last(),
        "is_verified_country_avg_debut_year",
    )
)
