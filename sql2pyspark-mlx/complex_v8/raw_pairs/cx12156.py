from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_country_by_release_da = Window.partitionBy("artist_country").orderBy("release_date", "id")
w_artist_country_by_release_da_2 = Window.partitionBy("artist_country").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_prior_6 = Window.orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 7)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
    )
)

combined_details = (
    base.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    combined_details
    .select(
        "*",
        F.lead("release_date").over(w_all_by_duration_sec).alias("next_release_date"),
        F.count(F.lit(1)).over(w_release_date_year).alias("rows_in_release_date_year"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_year), 2).alias("duration_sec_vs_release_date_year_avg"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_country_by_release_da)).alias("days_since_prev_release_date"),
        F.min("duration_sec").over(w_artist_country_by_release_da_2).alias("min_duration_sec_so_far"),
        F.max("duration_sec").over(w_all_by_duration_sec_prior_6).alias("duration_sec_max_6_prior"),
        F.max("duration_sec").over(w_artist_country_artist_id_by).alias("max_duration_sec_so_far"),
    )
)

result = (
    with_trends
    .filter(F.col("max_duration_sec_so_far") <= 3378)
    .select(
        "id",
        "duration_sec",
        "moods",
        "artist_country",
        "artist_is_verified",
        "next_release_date",
        "rows_in_release_date_year",
        "duration_sec_percent_rank",
        "min_duration_sec_so_far",
        "duration_sec_max_6_prior",
        "max_duration_sec_so_far",
    )
    .orderBy(F.col("artist_country").desc(), F.col("max_duration_sec_so_far").desc())
)
