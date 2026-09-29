from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_name_len_country_grou = Window.partitionBy("artist_name_len", "country_group").orderBy(F.col("debut_year").desc(), "id")
w_country_is_verified_by_media = Window.partitionBy("country", "is_verified").orderBy(F.col("median_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_name_len_country_grou_2 = Window.partitionBy("artist_name_len", "country_group").orderBy(F.col("debut_year").desc())
w_country_is_verified_by_media_2 = Window.partitionBy("country", "is_verified").orderBy(F.col("median_debut_year").desc(), "id")

base_artists = (
    df_artists
    .filter((~F.col("is_verified")) & (~F.col("genre").isin("Classical", "Jazz")))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        F.length(F.trim(F.col("artist_name"))).alias("artist_name_len"),
        F.when(F.col("country").isin("JP", "TH"), "group_a").when(F.col("country") == "KR", "group_b").otherwise("other").alias("country_group"),
    )
)

screened = (
    base_artists.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.is_explicit")) & (F.col("t.duration_sec") >= 165), "left_semi")
)

country_group_stats = (
    screened
    .groupBy("country_group")
    .agg(F.percentile_approx("debut_year", 0.5).alias("median_debut_year"))
)

above_average = (
    screened.alias("s")
    .join(country_group_stats.alias("g"), F.col("g.country_group") == F.col("s.country_group"), "inner")
    .filter(F.col("s.debut_year") >= F.col("g.median_debut_year"))
    .select(
        "s.*",
        "g.median_debut_year",
        F.round(F.col("s.debut_year") - F.col("g.median_debut_year"), 2).alias("debut_year_minus_median"),
    )
)

with_activity = (
    above_average.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-11-14").cast("date"))) & (F.size(F.col("t.moods")) >= 2), "left_semi")
    .filter(F.col("country").isin("JP", "KR", "TH"))
)

with_windows = (
    with_activity
    .select(
        "*",
        F.row_number().over(w_artist_name_len_country_grou).alias("debut_year_seq"),
        F.nth_value("artist_name_len", 2).over(w_country_is_verified_by_media).alias("second_artist_name_len"),
        F.rank().over(w_artist_name_len_country_grou_2).alias("debut_year_rank"),
        F.round(F.percent_rank().over(w_country_is_verified_by_media_2), 3).alias("median_debut_year_percent_rank"),
    )
)

per_group = (
    with_windows
    .groupBy("debut_year_seq")
    .agg(
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.round(F.avg("median_debut_year"), 2).alias("avg_median_debut_year"),
        F.avg("artist_name_len").alias("avg_artist_name_len_check"),
    )
    .filter(F.col("avg_artist_name_len_check") > 5)
    .drop("avg_artist_name_len_check")
)

result = (
    per_group
    .select(
        "debut_year_seq",
        F.when(F.col("debut_year_seq") >= 6, "large").when(F.col("debut_year_seq") >= 3, "medium").otherwise("small").alias("debut_year_seq_tier"),
    )
    .orderBy("debut_year_seq_tier", "debut_year_seq")
)
