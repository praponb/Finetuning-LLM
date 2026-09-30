from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_plus_1m = Window.orderBy("release_date_plus_1m", "id").rowsBetween(-3, -1)
w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", F.year("release_date"))
w_all_by_release_date_plus_1m_2 = Window.orderBy("release_date_plus_1m", "id").rowsBetween(-3, Window.currentRow)
w_artist_is_verified_release_d = Window.partitionBy("artist_is_verified", F.year("release_date")).orderBy("release_date")
w_all_by_release_date_plus_1m_3 = Window.orderBy("release_date_plus_1m", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_release_d_2 = Window.partitionBy("artist_is_verified", F.year("release_date")).orderBy("release_date", "id")
w_artist_is_verified_release_d_3 = Window.partitionBy("artist_is_verified", F.year("release_date")).orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("release_date")
w_release_date_plus_1m_month_e = Window.partitionBy(F.date_trunc("month", "release_date_plus_1m_month_end"), "artist_id")
w_artist_id_by_release_date_pl = Window.partitionBy("artist_id").orderBy(F.col("release_date_plus_1m").desc(), "id")
w_release_date_decile_by_relea = Window.partitionBy("release_date_decile").orderBy(F.col("release_date_cume_dist_2_adj").desc_nulls_first(), "id")

base = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.year(F.col("release_date")) == 2024))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.add_months(F.col("release_date"), 1).alias("release_date_plus_1m"),
    )
)

with_parent = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.release_date_plus_1m",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

sequenced = (
    with_parent
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "release_date_plus_1m",
        F.max("duration_sec").over(w_all_by_release_date_plus_1m).alias("duration_sec_max_3_prior"),
        F.round(F.avg("duration_sec").over(w_is_explicit_release_date_yea), 2).alias("is_explicit_release_date_year_avg_duration_sec"),
        F.max("duration_sec").over(w_all_by_release_date_plus_1m_2).alias("duration_sec_max_4"),
        F.round(F.cume_dist().over(w_artist_is_verified_release_d), 3).alias("release_date_cume_dist"),
        F.nth_value("duration_sec", 2).over(w_all_by_release_date_plus_1m_3).alias("second_duration_sec"),
        F.ntile(10).over(w_artist_is_verified_release_d_2).alias("release_date_decile"),
        F.round(F.percent_rank().over(w_artist_is_verified_release_d_2), 3).alias("release_date_percent_rank"),
        F.first("release_date_plus_1m", ignorenulls=True).over(w_artist_is_verified_release_d_3).alias("first_known_release_date_plus_1m"),
        F.round(F.cume_dist().over(w_is_explicit_artist_is_verifi), 3).alias("release_date_cume_dist_2"),
        F.sum("duration_sec").over(w_is_explicit_release_date_yea).alias("is_explicit_release_date_year_sum_duration_sec"),
    )
)

annotated = (
    sequenced
    .filter(F.col("duration_sec_max_3_prior") > 266)
    .select(
        "*",
        F.trunc(F.col("release_date_plus_1m"), "MM").alias("release_date_plus_1m_month_start"),
        F.round(F.col("release_date_cume_dist_2") * 1.5, 2).alias("release_date_cume_dist_2_adj"),
        F.last_day(F.col("release_date_plus_1m")).alias("release_date_plus_1m_month_end"),
    )
)

ranked = (
    annotated
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "release_date_plus_1m",
        "duration_sec_max_3_prior",
        "is_explicit_release_date_year_avg_duration_sec",
        "duration_sec_max_4",
        "release_date_cume_dist",
        "second_duration_sec",
        "release_date_decile",
        "release_date_percent_rank",
        "first_known_release_date_plus_1m",
        "release_date_cume_dist_2",
        "is_explicit_release_date_year_sum_duration_sec",
        "release_date_plus_1m_month_start",
        "release_date_cume_dist_2_adj",
        F.count(F.lit(1)).over(w_release_date_plus_1m_month_e).alias("rows_in_release_date_plus_1m_month_end_month_art"),
        F.round(F.percent_rank().over(w_artist_id_by_release_date_pl), 3).alias("release_date_plus_1m_percent_rank"),
        (F.col("duration_sec_max_4") - F.lag("duration_sec_max_4").over(w_release_date_decile_by_relea)).alias("duration_sec_max_4_change"),
        F.datediff(F.col("release_date_plus_1m"), F.lag("release_date_plus_1m").over(w_artist_id_by_release_date_pl)).alias("days_since_prev_release_date_plus_1m"),
        F.round(F.col("release_date_cume_dist") - F.avg("release_date_cume_dist").over(w_release_date_plus_1m_month_e), 2).alias("release_date_cume_dist_vs_release_date_plus_1m_m"),
        F.dense_rank().over(w_release_date_decile_by_relea).alias("release_date_cume_dist_2_adj_dense_rank"),
    )
)

grouped = (
    ranked
    .groupBy("artist_id", "is_explicit", "release_date_cume_dist_2_adj_dense_rank")
    .agg(
        F.sum("rows_in_release_date_plus_1m_month_end_month_art").alias("total_rows_in_release_date_plus_1m_month_end_mon"),
        F.min("release_date_plus_1m").alias("first_release_date_plus_1m"),
    )
)

result = (
    grouped
    .select(
        "artist_id",
        "release_date_cume_dist_2_adj_dense_rank",
        "total_rows_in_release_date_plus_1m_month_end_mon",
        "first_release_date_plus_1m",
    )
    .orderBy(F.col("artist_id").asc())
)
