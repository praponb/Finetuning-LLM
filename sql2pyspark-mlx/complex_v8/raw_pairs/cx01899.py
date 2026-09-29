from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_distance_prior_6 = Window.orderBy("bpm_distance", "id").rowsBetween(-6, -1)
w_all_by_bpm_distance = Window.orderBy("bpm_distance", "id")
w_all_by_bpm_distance_full = Window.orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_releas = Window.partitionBy("artist_is_verified").orderBy(F.col("release_date").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_is_verified_by_releas_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("release_date").desc())
w_release_date_month_is_explic_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit").orderBy("bpm_distance", "id")
w_artist_country_group_by_bpm = Window.partitionBy("artist_country_group").orderBy(F.col("bpm_distance_percent_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_artist_co = Window.partitionBy("artist_is_verified", "artist_country").orderBy("bpm_distance_avg_6_prior_per_artist_debut_year", "id")
w_artist_country_artist_id = Window.partitionBy("artist_country", "artist_id")
w_artist_is_verified_artist_co_2 = Window.partitionBy("artist_is_verified", "artist_country").orderBy("bpm_distance_avg_6_prior_per_artist_debut_year", "id").rowsBetween(-2, Window.currentRow)
w_all_by_bpm_distance_avg_6_pr = Window.orderBy("bpm_distance_avg_6_prior_per_artist_debut_year")

scoped = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.abs(F.col("bpm") - 104).alias("bpm_distance"),
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.days_since_release_date",
        "s.bpm_distance",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.last("bpm_distance").over(w_release_date_month_is_explic).alias("last_bpm_distance"),
        F.round(F.avg("bpm_distance").over(w_all_by_bpm_distance_prior_6), 2).alias("bpm_distance_avg_6_prior"),
        F.nth_value("release_date", 2).over(w_release_date_month_is_explic).alias("second_release_date"),
        F.round(F.percent_rank().over(w_all_by_bpm_distance), 3).alias("bpm_distance_percent_rank"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_bpm_distance_full).alias("second_artist_debut_year"),
        F.lead("days_since_release_date", 1, 0).over(w_all_by_bpm_distance).alias("next_days_since_release_date"),
        F.round(F.avg("days_since_release_date").over(w_artist_is_verified_by_releas), 2).alias("days_since_release_date_avg_7"),
        F.round(F.percent_rank().over(w_artist_is_verified_by_releas_2), 3).alias("release_date_percent_rank"),
        F.round(F.percent_rank().over(w_release_date_month_is_explic_2), 3).alias("bpm_distance_percent_rank_2"),
        F.nth_value("release_date", 2).over(w_all_by_bpm_distance_full).alias("second_release_date_3"),
    )
)

tagged = (
    windowed
    .select(
        "*",
        F.round(F.col("bpm_distance_avg_6_prior") / F.nullif(F.col("artist_debut_year"), F.lit(0)), 3).alias("bpm_distance_avg_6_prior_per_artist_debut_year"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.when(F.col("artist_country").isin("KR", "BR"), "group_a").when(F.col("artist_country") == "TH", "group_b").otherwise("other").alias("artist_country_group"),
    )
)

metrics = (
    tagged
    .select(
        "*",
        F.first("second_release_date", ignorenulls=True).over(w_artist_country_group_by_bpm).alias("first_known_second_release_date"),
        F.ntile(10).over(w_artist_is_verified_artist_co).alias("bpm_distance_avg_6_prior_per_artist_debut_year_d"),
        F.last("artist_debut_year", ignorenulls=True).over(w_artist_country_group_by_bpm).alias("last_known_artist_debut_year"),
        F.size(F.collect_set("artist_country_group").over(w_artist_country_artist_id)).alias("distinct_artist_country_group_in_artist_country_"),
        F.sum("artist_debut_year").over(w_artist_is_verified_artist_co_2).alias("artist_debut_year_sum_3"),
        F.round(F.percent_rank().over(w_all_by_bpm_distance_avg_6_pr), 3).alias("bpm_distance_avg_6_prior_per_artist_debut_year_p"),
    )
)

result = (
    metrics
    .filter(F.col("release_date") >= F.lit("2024-10-23").cast("date"))
    .select(
        "id",
        "artist_id",
        "moods",
        "days_since_release_date",
        "bpm_distance",
        "artist_is_verified",
        "last_bpm_distance",
        "bpm_distance_avg_6_prior",
        "second_release_date",
        "bpm_distance_percent_rank",
        "second_artist_debut_year",
        "next_days_since_release_date",
        "release_date_percent_rank",
        "bpm_distance_percent_rank_2",
        "second_release_date_3",
        "bpm_distance_avg_6_prior_per_artist_debut_year",
        "title_slug",
        "artist_country_group",
        "first_known_second_release_date",
        "bpm_distance_avg_6_prior_per_artist_debut_year_d",
        "last_known_artist_debut_year",
        "artist_debut_year_sum_3",
        "bpm_distance_avg_6_prior_per_artist_debut_year_p",
    )
    .orderBy(
        F.col("id").asc_nulls_last(),
        F.col("last_known_artist_debut_year").asc_nulls_last(),
    )
)
