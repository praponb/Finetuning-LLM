from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_or_unknown_is_verifi = Window.partitionBy("country_or_unknown", "is_verified")
w_genre_country_or_unknown_by = Window.partitionBy("genre", "country_or_unknown").orderBy("last_track_release_date_2")
w_genre_country_or_unknown_by_2 = Window.partitionBy("genre", "country_or_unknown").orderBy("last_track_release_date_2", "id")

prepared_artists = (
    df_artists
    .filter(F.col("debut_year").between(1994, 2023))
    .select(
        "id",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.abs(F.col("debut_year") - 2006).alias("debut_year_distance"),
        F.when(F.col("genre").isin("Classical", "Indie"), "group_a").when(F.col("genre") == "Pop", "group_b").otherwise("other").alias("genre_group"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country_or_unknown"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("release_date").alias("last_track_release_date"),
        F.max("release_date").alias("last_track_release_date_2"),
        F.min("bpm").alias("min_track_bpm"),
    )
)

with_counts = (
    prepared_artists.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.last_track_release_date",
        "t.last_track_release_date_2",
        "t.min_track_bpm",
    )
)

ranked = (
    with_counts
    .select(
        "*",
        F.sum("min_track_bpm").over(w_country_or_unknown_is_verifi).alias("country_or_unknown_is_verified_sum_min_track_bpm"),
        F.round(F.cume_dist().over(w_genre_country_or_unknown_by), 3).alias("last_track_release_date_2_cume_dist"),
        F.ntile(4).over(w_genre_country_or_unknown_by_2).alias("last_track_release_date_2_quartile"),
        F.lag("last_track_release_date_2", 3).over(w_genre_country_or_unknown_by_2).alias("prev3_last_track_release_date_2"),
        F.round(F.col("min_track_bpm") - F.avg("min_track_bpm").over(w_country_or_unknown_is_verifi), 2).alias("min_track_bpm_vs_country_or_unknown_is_verified_"),
        (F.col("min_track_bpm") - F.lag("min_track_bpm").over(w_genre_country_or_unknown_by_2)).alias("min_track_bpm_change"),
    )
)

summary = (
    ranked
    .groupBy(
        F.date_trunc("month", "last_track_release_date").alias("last_track_release_date_month"),
        F.when(F.col("min_track_bpm") >= 156, "HIGH").when(F.col("min_track_bpm") >= 102, "MEDIUM").otherwise("LOW").alias("min_track_bpm_tier"),
    )
    .agg(
        F.countDistinct("country").alias("distinct_countrys"),
        F.round(100 * F.count(F.when(~F.col("genre").isin("HipHop", "Indie"), 1)) / F.count("*"), 1).alias("pct_genre_not_in_hiphop_indie"),
        F.countDistinct("country_or_unknown").alias("distinct_country_or_unknowns"),
        F.array_sort(F.collect_set("genre")).alias("genre_list"),
        F.count("*").alias("n_rows"),
    )
)

exploded = (
    summary
    .select(
        "last_track_release_date_month",
        "min_track_bpm_tier",
        "distinct_countrys",
        "pct_genre_not_in_hiphop_indie",
        "distinct_country_or_unknowns",
        "n_rows",
        F.explode("genre_list").alias("genre_list_item"),
    )
)

result = (
    exploded
    .select(
        "last_track_release_date_month",
        "min_track_bpm_tier",
        "pct_genre_not_in_hiphop_indie",
        "distinct_country_or_unknowns",
        "genre_list_item",
    )
    .orderBy(
        F.col("genre_list_item").desc(),
        "last_track_release_date_month",
        "min_track_bpm_tier",
    )
    .limit(5)
)
