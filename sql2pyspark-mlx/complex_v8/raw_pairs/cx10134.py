from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_verified_first_track_rele = Window.partitionBy("is_verified", F.month("first_track_release_date"))
w_country_or_unknown_by_track = Window.partitionBy("country_or_unknown").orderBy(F.col("track_count").desc(), "id").rowsBetween(-1, -1)
w_country_or_unknown_by_track_2 = Window.partitionBy("country_or_unknown").orderBy(F.col("track_count").desc())
w_country_or_unknown_by_track_3 = Window.partitionBy("country_or_unknown").orderBy(F.col("track_count").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_artists
    .filter((~F.col("is_verified")) & (F.col("country").isNotNull()))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.upper(F.trim(F.col("genre"))).alias("genre_clean"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.min("release_date").alias("first_track_release_date"),
    )
)

profiled = (
    filtered.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.first_track_release_date",
    )
)

ranked = (
    profiled
    .select(
        "id",
        "artist_name",
        "genre",
        "is_verified",
        "country_or_unknown",
        "debut_year_filled",
        "genre_clean",
        "track_count",
        F.min("track_count").over(w_is_verified_first_track_rele).alias("is_verified_first_track_release_date_month_min_t"),
        F.sum("track_count").over(w_country_or_unknown_by_track).alias("track_count_sum_1_prior"),
        F.dense_rank().over(w_country_or_unknown_by_track_2).alias("track_count_dense_rank"),
        F.sum("debut_year_filled").over(w_country_or_unknown_by_track_3).alias("running_debut_year_filled"),
    )
)

with_children_tracks = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), F.col("t.artist_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_name",
        "s.genre",
        "s.debut_year_filled",
        "s.is_verified_first_track_release_date_month_min_t",
        "s.track_count_sum_1_prior",
        "s.running_debut_year_filled",
        F.col("t.id").alias("track_id"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

result = (
    with_children_tracks
    .rollup("genre", "track_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("track_title")).alias("track_title_list"),
        F.sum("track_count_sum_1_prior").alias("total_track_count_sum_1_prior"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
    )
    .select(
        F.coalesce(F.col("genre"), F.lit("ALL")).alias("genre"),
        "track_id",
        "grouping_level",
        "track_title_list",
        "total_track_count_sum_1_prior",
        "genre_list",
    )
    .orderBy(F.col("genre").asc_nulls_last(), F.col("track_id").asc_nulls_last())
)
