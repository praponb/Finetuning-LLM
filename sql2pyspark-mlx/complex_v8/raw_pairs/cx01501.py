from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_release_date_prior_6 = Window.orderBy("release_date", "id").rowsBetween(-6, -1)
w_artist_country_by_bpm_runnin = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_release_date = Window.orderBy("release_date", "id")
w_is_explicit_artist_country_b = Window.partitionBy("is_explicit", "artist_country").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("bpm_filled").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy(F.col("bpm").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_country_artist_id_by_2 = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("bpm_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date"))
w_all_by_duration_sec_moving_6 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("bpm") > 100))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

combined_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_filled",
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.lag("release_date").over(w_all_by_duration_sec).alias("prev_release_date"),
        F.round(F.avg("bpm").over(w_all_by_release_date_prior_6), 2).alias("bpm_avg_6_prior"),
        F.first("release_date", ignorenulls=True).over(w_artist_country_by_bpm_runnin).alias("first_known_release_date"),
        F.lead("release_date").over(w_all_by_release_date).alias("next_release_date"),
        F.max("duration_sec").over(w_is_explicit_artist_country_b).alias("max_duration_sec_so_far"),
        F.sum("duration_sec").over(w_artist_country_artist_id_by).alias("duration_sec_sum_2_prior"),
        F.sum("bpm_filled").over(w_is_explicit_artist_country_b).alias("running_bpm_filled"),
        F.round(F.cume_dist().over(w_artist_country_by_bpm), 3).alias("bpm_cume_dist"),
        F.min("bpm").over(w_artist_country_by_bpm_runnin).alias("min_bpm_so_far"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.first("bpm_filled").over(w_artist_country_artist_id_by_2).alias("first_bpm_filled"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit_release_date_mon), 2).alias("bpm_vs_is_explicit_release_date_month_avg"),
        F.size(F.collect_set("artist_country").over(w_is_explicit)).alias("distinct_artist_country_in_is_explicit"),
        F.round(F.avg("bpm").over(w_all_by_duration_sec_moving_6), 2).alias("bpm_avg_7"),
    )
)

result = (
    ranked
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "bpm_filled",
        "artist_country",
        "prev_release_date",
        "first_known_release_date",
        "next_release_date",
        "max_duration_sec_so_far",
        "running_bpm_filled",
        "bpm_cume_dist",
        "min_bpm_so_far",
        "rows_in_is_explicit",
        "bpm_vs_is_explicit_release_date_month_avg",
        "distinct_artist_country_in_is_explicit",
        "bpm_avg_7",
    )
    .orderBy(F.col("prev_release_date").asc_nulls_last())
)
