from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")
w_artist_country_moods_count_b = Window.partitionBy("artist_country", "moods_count").orderBy("artist_debut_year", "id")
w_artist_country_moods_count_b_2 = Window.partitionBy("artist_country", "moods_count").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_moods_count_b_3 = Window.partitionBy("artist_country", "moods_count").orderBy("artist_debut_year", "id").rowsBetween(-1, -1)

cleaned = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "chill")) & (F.col("artist_id") <= 9))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_details_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.moods_count",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    with_details_artist
    .select(
        "id",
        "is_explicit",
        "artist_country",
        "artist_debut_year",
        F.size(F.collect_set("artist_country").over(w_is_explicit)).alias("distinct_artist_country_in_is_explicit"),
        F.round(F.percent_rank().over(w_artist_country_moods_count_b), 3).alias("artist_debut_year_percent_rank"),
        F.first("moods_count", ignorenulls=True).over(w_artist_country_moods_count_b_2).alias("first_known_moods_count"),
        F.round(F.col("moods_count") / F.nullif(F.sum("moods_count").over(w_is_explicit), F.lit(0)), 4).alias("moods_count_share_of_is_explicit"),
        F.round(F.avg("moods_count").over(w_artist_country_moods_count_b_3), 2).alias("moods_count_avg_1_prior"),
        F.ntile(5).over(w_artist_country_moods_count_b).alias("artist_debut_year_quintile"),
        F.max("artist_debut_year").over(w_is_explicit).alias("is_explicit_max_artist_debut_year"),
    )
)

enriched = (
    with_trends
    .filter(F.col("artist_debut_year_quintile") <= 9)
    .select(
        "id",
        "is_explicit",
        "artist_country",
        "artist_debut_year",
        "distinct_artist_country_in_is_explicit",
        "artist_debut_year_percent_rank",
        "first_known_moods_count",
        "moods_count_share_of_is_explicit",
        "moods_count_avg_1_prior",
        "is_explicit_max_artist_debut_year",
        (F.col("moods_count_avg_1_prior") - F.col("artist_debut_year")).alias("moods_count_avg_1_prior_minus_artist_debut_year"),
        F.abs(F.col("moods_count_avg_1_prior") - 1.6).alias("moods_count_avg_1_prior_distance"),
    )
)

result = (
    enriched
    .select("artist_country", "artist_debut_year", "artist_debut_year_percent_rank")
    .distinct()
    .orderBy(F.col("artist_debut_year").desc())
)
