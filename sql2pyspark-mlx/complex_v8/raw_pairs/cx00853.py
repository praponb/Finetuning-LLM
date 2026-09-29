from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy("release_date", "id")
w_is_explicit_by_artist_debut = Window.partitionBy("is_explicit").orderBy("artist_debut_year", "id")
w_artist_country = Window.partitionBy("artist_country")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_is_explicit_by_artist_debut_2 = Window.partitionBy("is_explicit").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_is_explicit_by_artist_debut_3 = Window.partitionBy("is_explicit").orderBy("artist_debut_year")
w_n_rows_artist_country_by_n_r = Window.partitionBy("n_rows", "artist_country").orderBy("n_rows", "release_date_month", "artist_country")
w_distinct_artist_countrys_art = Window.partitionBy("distinct_artist_countrys", "artist_country")

recent_tracks = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-11-14").cast("date")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
    )
)

combined_details_artist = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

windowed = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "moods",
        "artist_debut_year",
        "artist_country",
        F.lead("artist_debut_year", 2, 0).over(w_all_by_release_date).alias("next2_artist_debut_year"),
        F.ntile(3).over(w_artist_id_artist_country_by).alias("release_date_tercile"),
        F.lag("release_date").over(w_is_explicit_by_artist_debut).alias("prev_release_date"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_country), 2).alias("artist_debut_year_vs_artist_country_avg"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_artist_debut)).alias("days_since_prev_release_date"),
        F.min("artist_debut_year").over(w_release_date_year_artist_id).alias("release_date_year_artist_id_min_artist_debut_yea"),
        F.min("artist_debut_year").over(w_is_explicit_by_artist_debut_2).alias("min_artist_debut_year_so_far"),
        F.round(F.cume_dist().over(w_is_explicit_by_artist_debut), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_artist_debut_year), 3).alias("artist_debut_year_cume_dist_2"),
        F.round(F.avg("artist_debut_year").over(w_artist_country), 2).alias("artist_country_avg_artist_debut_year"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_release_date_year_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_release_date_year_art"),
        F.rank().over(w_is_explicit_by_artist_debut_3).alias("artist_debut_year_rank"),
        F.count(F.lit(1)).over(w_release_date_year_artist_id).alias("rows_in_release_date_year_artist_id"),
        F.sum("artist_debut_year").over(w_release_date_year_artist_id).alias("release_date_year_artist_id_sum_artist_debut_yea"),
    )
)

grouped = (
    windowed
    .filter(F.col("release_date") >= F.lit("2024-08-26").cast("date"))
    .groupBy(
        F.date_trunc("month", "release_date").alias("release_date_month"),
        "artist_country",
    )
    .agg(
        F.min("artist_debut_year_share_of_release_date_year_art").alias("min_artist_debut_year_share_of_release_date_year"),
        F.countDistinct("artist_country").alias("distinct_artist_countrys"),
        F.count("*").alias("n_rows"),
    )
)

sequenced = (
    grouped
    .select(
        "release_date_month",
        "artist_country",
        "min_artist_debut_year_share_of_release_date_year",
        F.round(F.percent_rank().over(w_n_rows_artist_country_by_n_r), 3).alias("n_rows_percent_rank"),
        F.rank().over(w_n_rows_artist_country_by_n_r).alias("n_rows_rank"),
        F.count(F.lit(1)).over(w_distinct_artist_countrys_art).alias("rows_in_distinct_artist_countrys_artist_country"),
        F.round(F.avg("distinct_artist_countrys").over(w_distinct_artist_countrys_art), 2).alias("distinct_artist_countrys_artist_country_avg_dist"),
    )
)

result = (
    sequenced
    .select(
        "release_date_month",
        "artist_country",
        "n_rows_percent_rank",
        "rows_in_distinct_artist_countrys_artist_country",
        "distinct_artist_countrys_artist_country_avg_dist",
        F.lower(F.regexp_replace(F.trim(F.col("artist_country")), " ", "_")).alias("artist_country_slug"),
        (F.col("n_rows_percent_rank") - F.col("n_rows_rank")).alias("n_rows_percent_rank_minus_n_rows_rank"),
    )
    .orderBy(
        F.col("n_rows_percent_rank_minus_n_rows_rank").desc(),
        F.col("rows_in_distinct_artist_countrys_artist_country").desc(),
    )
)
