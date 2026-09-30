from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_genre_or_unknown_genre_by_de = Window.partitionBy("genre_or_unknown", "genre").orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_genre_or_unknown_genre_by_de_2 = Window.partitionBy("genre_or_unknown", "genre").orderBy(F.col("debut_year_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_genre_genre_or_unknown = Window.partitionBy("genre", "genre_or_unknown")
w_genre_or_unknown_genre_by_de_3 = Window.partitionBy("genre_or_unknown", "genre").orderBy(F.col("debut_year_filled").desc(), "id")
w_genre_or_unknown = Window.partitionBy("genre_or_unknown")
w_seq_no_genre_or_unknown_by_d = Window.partitionBy("seq_no", "genre_or_unknown").orderBy("debut_year_filled", "id")
w_country_genre_or_unknown_by = Window.partitionBy("country", "genre_or_unknown").orderBy("first_debut_year_filled")

valid_artists = (
    df_artists
    .filter(F.col("debut_year") >= 1997)
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.lower(F.trim(F.col("country"))).alias("country_clean"),
    )
)

without_matches = (
    valid_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (~F.col("t.is_explicit")) & (F.col("t.bpm") <= 156), "left_anti")
    .filter(F.col("country") == "JP")
)

tagged = (
    without_matches
    .filter(F.col("debut_year_filled") < 2020)
    .select(
        "*",
        F.concat_ws("|", F.col("country"), F.col("artist_name")).alias("country_artist_name_label"),
        F.substring(F.trim(F.col("country")), 1, 3).alias("country_prefix"),
        F.coalesce(F.col("genre"), F.lit("UNKNOWN")).alias("genre_or_unknown"),
    )
)

with_windows = (
    tagged
    .select(
        "*",
        F.count(F.lit(1)).over(w_genre_or_unknown_genre_by_de).alias("running_count"),
        F.first("debut_year_filled").over(w_genre_or_unknown_genre_by_de_2).alias("first_debut_year_filled"),
        F.nth_value("debut_year_filled", 2).over(w_genre_or_unknown_genre_by_de_2).alias("second_debut_year_filled"),
        F.min("debut_year_filled").over(w_genre_genre_or_unknown).alias("genre_genre_or_unknown_min_debut_year_filled"),
        F.round(F.col("debut_year_filled") - F.avg("debut_year_filled").over(w_genre_genre_or_unknown), 2).alias("debut_year_filled_vs_genre_genre_or_unknown_avg"),
        F.row_number().over(w_genre_or_unknown_genre_by_de_3).alias("seq_no"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year_filled",
        "country_clean",
        "country_artist_name_label",
        "country_prefix",
        "running_count",
        "debut_year_filled_vs_genre_genre_or_unknown_avg",
        "seq_no",
        F.sum("second_debut_year_filled").over(w_genre_or_unknown).alias("genre_or_unknown_sum_second_debut_year_filled"),
        F.row_number().over(w_seq_no_genre_or_unknown_by_d).alias("rn"),
        F.round(F.percent_rank().over(w_country_genre_or_unknown_by), 3).alias("first_debut_year_filled_percent_rank"),
    )
)

grouped = (
    with_trends
    .groupBy("country", "genre", "rn")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("country").alias("distinct_countrys"),
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
    )
)

result = (
    grouped
    .filter(F.col("genre").isNotNull())
    .select(
        "country",
        "genre",
        "row_count",
        "artist_name_list",
        F.when(F.col("genre").isin("Pop", "Jazz"), "group_a").when(F.col("genre") == "Classical", "group_b").otherwise("other").alias("genre_group"),
        F.when(F.col("row_count") >= 8, "HIGH").when(F.col("row_count") >= 3, "MEDIUM").otherwise("LOW").alias("row_count_tier"),
    )
    .orderBy("country")
)
