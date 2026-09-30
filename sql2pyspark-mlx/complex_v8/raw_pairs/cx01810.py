from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_by_release = Window.partitionBy(F.year("release_date")).orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_release_date_month_artist_co = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_country").orderBy(F.col("release_date").desc())
w_all_by_release_date_full = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_co_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_country").orderBy(F.col("release_date").desc(), "id")
w_all_by_release_date_2 = Window.orderBy("release_date", "id")
w_all_by_release_date_full_2 = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_co_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_country").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_co_4 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_country").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_second_release_date_month_to = Window.partitionBy(F.to_date("second_release_date_month")).orderBy(F.col("n_is_explicit_false").desc())
w_second_release_date_month_to_2 = Window.partitionBy(F.to_date("second_release_date_month")).orderBy(F.col("n_is_explicit_false").desc(), "second_release_date_month")
w_distinct_ids_n_is_explicit_f = Window.partitionBy("distinct_ids", "n_is_explicit_false")

scoped = (
    df_tracks
    .filter((F.col("bpm") < 143) & (F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

with_parent_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.is_explicit",
        F.col("a.country").alias("artist_country"),
    )
)

sequenced = (
    with_parent_artist
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "artist_country",
        F.row_number().over(w_release_date_year_by_release).alias("rn"),
        F.lead("release_date").over(w_all_by_release_date).alias("next_release_date"),
        F.rank().over(w_release_date_month_artist_co).alias("release_date_rank"),
        F.lag("release_date", 2).over(w_all_by_release_date).alias("prev2_release_date"),
        F.nth_value("release_date", 2).over(w_all_by_release_date_full).alias("second_release_date"),
        F.round(F.cume_dist().over(w_release_date_month_artist_co), 3).alias("release_date_cume_dist"),
        F.lead("release_date").over(w_release_date_month_artist_co_2).alias("next_release_date_2"),
        F.lead("release_date", 2).over(w_all_by_release_date_2).alias("next2_release_date"),
        F.first("release_date").over(w_all_by_release_date_full_2).alias("first_release_date"),
        F.first("release_date").over(w_release_date_month_artist_co_3).alias("first_release_date_2"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_release_date_2)).alias("days_since_prev_release_date"),
        F.first("release_date", ignorenulls=True).over(w_release_date_month_artist_co_4).alias("first_known_release_date"),
        F.first("release_date").over(w_all_by_release_date_full).alias("first_release_date_3"),
        F.lead("release_date", 3).over(w_all_by_release_date_2).alias("next3_release_date"),
        F.ntile(4).over(w_all_by_release_date_2).alias("release_date_quartile"),
        F.row_number().over(w_all_by_release_date_2).alias("rn_2"),
    )
)

stats = (
    sequenced
    .filter(F.year(F.col("first_release_date_2")) == 2024)
    .groupBy(
        F.date_trunc("month", "second_release_date").alias("second_release_date_month"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.sum(F.when(~F.col("is_explicit"), F.col("release_date_cume_dist")).otherwise(0)).alias("release_date_cume_dist_is_explicit_false"),
        F.sum("release_date_quartile").alias("total_release_date_quartile"),
    )
)

windowed = (
    stats
    .select(
        "second_release_date_month",
        "n_is_explicit_false",
        "release_date_cume_dist_is_explicit_false",
        "total_release_date_quartile",
        F.round(F.cume_dist().over(w_second_release_date_month_to), 3).alias("n_is_explicit_false_cume_dist"),
        F.row_number().over(w_second_release_date_month_to_2).alias("rn"),
        F.sum("distinct_ids").over(w_distinct_ids_n_is_explicit_f).alias("distinct_ids_n_is_explicit_false_sum_distinct_id"),
        F.round(F.col("n_is_explicit_false") / F.nullif(F.sum("n_is_explicit_false").over(w_distinct_ids_n_is_explicit_f), F.lit(0)), 4).alias("n_is_explicit_false_share_of_distinct_ids_n_is_e"),
    )
)

result = (
    windowed
    .select(
        "second_release_date_month",
        "n_is_explicit_false",
        "release_date_cume_dist_is_explicit_false",
        "total_release_date_quartile",
        "n_is_explicit_false_cume_dist",
        "rn",
        "n_is_explicit_false_share_of_distinct_ids_n_is_e",
        F.round(F.col("rn") / F.nullif(F.col("n_is_explicit_false"), F.lit(0)), 3).alias("rn_per_n_is_explicit_false"),
        (F.floor(F.col("rn") / 2) * 2).alias("rn_bucket"),
    )
    .orderBy(F.col("rn_bucket").desc_nulls_first())
)
