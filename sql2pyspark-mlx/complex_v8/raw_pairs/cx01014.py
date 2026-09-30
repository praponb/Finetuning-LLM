from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_moving_6 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_genre_release_date_ye = Window.partitionBy("artist_genre", F.year("release_date")).orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_2 = Window.orderBy("duration_sec", "id")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.date_trunc("month", "release_date"))
w_artist_genre_release_date_ye_2 = Window.partitionBy("artist_genre", F.year("release_date")).orderBy(F.col("duration_sec").desc())
w_artist_genre_release_date_ye_3 = Window.partitionBy("artist_genre", F.year("release_date")).orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_country_by_release_da = Window.partitionBy("artist_country").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_all_by_duration_sec_full = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_duration_sec_pe = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_percent_rank").desc(), "id")
w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_release_date = Window.orderBy("release_date", "id")

prepared = df_tracks.filter(F.col("title").isNotNull())

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

ranked = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "artist_genre",
        "artist_country",
        F.max("duration_sec").over(w_all_by_duration_sec_moving_6).alias("duration_sec_max_7"),
        F.row_number().over(w_artist_genre_release_date_ye).alias("duration_sec_seq"),
        F.lag("release_date", 2).over(w_all_by_duration_sec).alias("prev2_release_date"),
        F.ntile(5).over(w_all_by_duration_sec_2).alias("duration_sec_quintile"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_release_date_mon), F.lit(0)), 4).alias("bpm_share_of_is_explicit_release_date_month"),
        F.round(F.cume_dist().over(w_artist_genre_release_date_ye_2), 3).alias("duration_sec_cume_dist"),
        F.max("duration_sec").over(w_artist_genre_release_date_ye_3).alias("max_duration_sec_so_far"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_is_explicit"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_country_by_release_da)).alias("duration_sec_change"),
        F.nth_value("duration_sec", 2).over(w_all_by_duration_sec_full).alias("second_duration_sec"),
    )
)

leaders = (
    ranked
    .filter(F.col("duration_sec_seq") == 1)
    .drop("duration_sec_seq")
)

metrics = (
    leaders
    .select(
        "*",
        F.row_number().over(w_artist_id_by_duration_sec_pe).alias("seq_no"),
        F.ntile(5).over(w_artist_id_by_duration_sec_pe).alias("duration_sec_percent_rank_quintile"),
        F.max("max_duration_sec_so_far").over(w_artist_genre).alias("artist_genre_max_max_duration_sec_so_far"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.min("duration_sec_percent_rank").over(w_artist_genre).alias("artist_genre_min_duration_sec_percent_rank"),
    )
)

totals = (
    metrics
    .filter(F.col("release_date_rank") >= 2)
    .groupBy(
        "release_date_rank",
        F.when(F.col("duration_sec_max_7") >= 909, "large").when(F.col("duration_sec_max_7") >= 558, "medium").otherwise("small").alias("duration_sec_max_7_tier"),
        "artist_genre",
    )
    .agg(
        F.sum("duration_sec_percent_rank").alias("total_duration_sec_percent_rank"),
        F.sum(F.when(~F.col("is_explicit"), F.col("duration_sec_rank"))).alias("duration_sec_rank_not_is_explicit"),
        F.count(F.when(F.col("artist_genre") != "Pop", 1)).alias("n_artist_genre_pop"),
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
        F.sum("duration_sec_quintile").alias("sum_duration_sec_quintile_check"),
    )
    .filter(F.col("sum_duration_sec_quintile_check") > 4)
    .drop("sum_duration_sec_quintile_check")
)

result = (
    totals
    .select(
        "release_date_rank",
        "duration_sec_max_7_tier",
        "artist_genre",
        "total_duration_sec_percent_rank",
        "n_artist_genre_pop",
        "distinct_artist_genres",
    )
    .orderBy(F.col("artist_genre").desc_nulls_first())
)
