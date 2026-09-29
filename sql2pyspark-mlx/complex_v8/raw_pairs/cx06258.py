from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_is_explicit_by = Window.partitionBy("artist_genre", "is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_artist_genre_by_du = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec_filled", "id").rowsBetween(-3, Window.currentRow)
w_artist_id_artist_genre_by_du_2 = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec_filled", "id")
w_all_by_release_date_ties = Window.orderBy(F.col("release_date").desc())
w_artist_genre = Window.partitionBy("artist_genre")
w_artist_id_artist_genre_by_du_3 = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec_filled", "id").rowsBetween(-2, -1)
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_running = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_genre_by_du_4 = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm_avg_4_pri = Window.partitionBy("is_explicit").orderBy(F.col("bpm_avg_4").desc(), "id").rowsBetween(-3, -1)
w_artist_genre_is_explicit = Window.partitionBy("artist_genre", "is_explicit")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_artist_genre_by_release_date_2 = Window.partitionBy("artist_genre").orderBy("release_date")
w_is_explicit_by_bpm_avg_4 = Window.partitionBy("is_explicit").orderBy(F.col("bpm_avg_4").desc(), "id")
w_artist_genre_by_release_date_3 = Window.partitionBy("artist_genre").orderBy("release_date", "id").rowsBetween(-2, -1)
w_distinct_ids_by_first_bpm_2 = Window.partitionBy("distinct_ids").orderBy(F.col("first_bpm_2_sum_3_prior_is_explicit_false").desc())
w_distinct_ids_by_first_bpm_2_2 = Window.partitionBy("distinct_ids").orderBy(F.col("first_bpm_2_sum_3_prior_is_explicit_false").desc(), "release_date_ym", "artist_genre")

cleaned_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.abs(F.col("bpm") - 118).alias("bpm_distance"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

joined_artist = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.bpm_distance",
        "s.duration_sec_filled",
        "a.artist_name",
        F.col("a.genre").alias("artist_genre"),
    )
)

with_windows = (
    joined_artist
    .select(
        "*",
        F.nth_value("duration_sec_filled", 2).over(w_artist_genre_is_explicit_by).alias("second_duration_sec_filled"),
        F.round(F.avg("bpm").over(w_artist_id_artist_genre_by_du), 2).alias("bpm_avg_4"),
        F.round(F.percent_rank().over(w_artist_id_artist_genre_by_du_2), 3).alias("duration_sec_filled_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_release_date_ties), 3).alias("release_date_percent_rank"),
        F.max("duration_sec_filled").over(w_artist_genre).alias("artist_genre_max_duration_sec_filled"),
        F.round(F.avg("bpm").over(w_artist_id_artist_genre_by_du_3), 2).alias("bpm_avg_2_prior"),
        F.lead("duration_sec_filled").over(w_all_by_release_date).alias("next_duration_sec_filled"),
        F.last("duration_sec_filled", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_duration_sec_filled"),
        F.first("bpm").over(w_artist_genre_is_explicit_by).alias("first_bpm"),
        F.count(F.lit(1)).over(w_artist_genre).alias("rows_in_artist_genre"),
        F.first("bpm").over(w_artist_id_artist_genre_by_du_4).alias("first_bpm_2"),
    )
)

derived = (
    with_windows
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "moods",
        "bpm_distance",
        "duration_sec_filled",
        "artist_genre",
        "second_duration_sec_filled",
        "bpm_avg_4",
        "last_known_duration_sec_filled",
        "first_bpm_2",
        F.lower(F.regexp_replace(F.trim(F.col("artist_genre")), " ", "_")).alias("artist_genre_slug"),
        F.array_contains(F.col("moods"), "party").alias("has_party"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

sequenced = (
    derived
    .select(
        "*",
        F.sum("first_bpm_2").over(w_is_explicit_by_bpm_avg_4_pri).alias("first_bpm_2_sum_3_prior"),
        F.sum("second_duration_sec_filled").over(w_artist_genre_is_explicit).alias("artist_genre_is_explicit_sum_second_duration_sec"),
        F.sum("first_bpm_2").over(w_artist_genre_by_release_date).alias("first_bpm_2_sum_7"),
        F.round(F.percent_rank().over(w_artist_genre_by_release_date_2), 3).alias("release_date_percent_rank"),
        F.sum("bpm_distance").over(w_artist_genre_is_explicit).alias("artist_genre_is_explicit_sum_bpm_distance"),
        F.lead("second_duration_sec_filled").over(w_is_explicit_by_bpm_avg_4).alias("next_second_duration_sec_filled"),
        F.sum("duration_sec_filled").over(w_artist_genre_by_release_date_3).alias("duration_sec_filled_sum_2_prior"),
    )
)

totals = (
    sequenced
    .groupBy(
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
        "artist_genre",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("row_count"),
        F.sum(F.when(~F.col("is_explicit"), F.col("first_bpm_2_sum_3_prior"))).alias("first_bpm_2_sum_3_prior_is_explicit_false"),
        F.max("bpm_avg_4").alias("max_bpm_avg_4_check"),
    )
    .filter(F.col("max_bpm_avg_4_check") > 84.0)
    .drop("max_bpm_avg_4_check")
)

ranked = (
    totals
    .withColumn("first_bpm_2_sum_3_prior_is_explicit_false_dense_", F.dense_rank().over(w_distinct_ids_by_first_bpm_2))
    .withColumn("first_bpm_2_sum_3_prior_is_explicit_false_quarti", F.ntile(4).over(w_distinct_ids_by_first_bpm_2_2))
)

result = (
    ranked
    .select(
        "release_date_ym",
        "artist_genre",
        "distinct_ids",
        "row_count",
        "first_bpm_2_sum_3_prior_is_explicit_false",
        "first_bpm_2_sum_3_prior_is_explicit_false_dense_",
        "first_bpm_2_sum_3_prior_is_explicit_false_quarti",
    )
    .orderBy(
        F.col("first_bpm_2_sum_3_prior_is_explicit_false_dense_").desc_nulls_first(),
    )
)
