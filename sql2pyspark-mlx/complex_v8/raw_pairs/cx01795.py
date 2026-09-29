from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_prior_1 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, -1)
w_artist_id = Window.partitionBy("artist_id")
w_release_date_plus_90d_month = Window.partitionBy(F.date_trunc("month", "release_date_plus_90d"))
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy("release_date_plus_90d", "id")
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").asc_nulls_last(), "id")
w_all_by_duration_sec_2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w_release_date_plus_90d_rank = Window.partitionBy("release_date_plus_90d_rank")
w_artist_country_release_date = Window.partitionBy("artist_country", "release_date_plus_90d_rank").orderBy("release_date_plus_90d_month_min_duration_sec")
w_all_by_duration_sec_cume_dis = Window.orderBy(F.col("duration_sec_cume_dist").desc(), "id")
w_all_by_duration_sec_cume_dis_2 = Window.orderBy(F.col("duration_sec_cume_dist").desc())
w_artist_country_release_date_2 = Window.partitionBy("artist_country", "release_date_plus_90d_rank").orderBy("release_date_plus_90d_month_min_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_release_date_3 = Window.partitionBy("artist_country", "release_date_plus_90d_rank").orderBy("release_date_plus_90d_month_min_duration_sec", "id")
w_is_explicit_by_n_not_is_expl = Window.partitionBy("is_explicit").orderBy("n_not_is_explicit", "is_explicit", "duration_sec_cume_dist_rank", "release_date_plus_90d_month_min_duration_sec_den").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter((F.col("bpm") >= 96) & (F.col("artist_id") <= 12))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 90).alias("release_date_plus_90d"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

ranked = (
    joined_artist
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_all_by_bpm_prior_1), 2).alias("duration_sec_avg_1_prior"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_plus_90d_month), 2).alias("duration_sec_vs_release_date_plus_90d_month_avg"),
        F.rank().over(w_artist_id_artist_country_by).alias("release_date_plus_90d_rank"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.sum("bpm").over(w_release_date_plus_90d_month).alias("release_date_plus_90d_month_sum_bpm"),
        F.ntile(3).over(w_all_by_bpm).alias("bpm_tercile"),
        F.datediff(F.col("release_date_plus_90d"), F.lag("release_date_plus_90d").over(w_all_by_duration_sec)).alias("days_since_prev_release_date_plus_90d"),
        F.min("duration_sec").over(w_release_date_plus_90d_month).alias("release_date_plus_90d_month_min_duration_sec"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_2), 3).alias("duration_sec_cume_dist"),
    )
)

enriched = (
    ranked
    .filter(~F.col("artist_is_verified"))
    .withColumn("days_since_release_date_plus_90d", F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date_plus_90d")))
)

ranked_2 = (
    enriched
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "moods",
        "release_date_plus_90d",
        "release_date_label",
        "artist_is_verified",
        "duration_sec_avg_1_prior",
        "duration_sec_vs_artist_id_avg",
        "release_date_plus_90d_rank",
        "bpm_vs_artist_id_avg",
        "days_since_prev_release_date_plus_90d",
        "days_since_release_date_plus_90d",
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_plus_90d_rank), 2).alias("duration_sec_vs_release_date_plus_90d_rank_avg"),
        F.dense_rank().over(w_artist_country_release_date).alias("release_date_plus_90d_month_min_duration_sec_den"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_cume_dis), 3).alias("duration_sec_cume_dist_percent_rank"),
        F.rank().over(w_all_by_duration_sec_cume_dis_2).alias("duration_sec_cume_dist_rank"),
        F.count(F.lit(1)).over(w_artist_country_release_date_2).alias("running_count"),
        F.count(F.lit(1)).over(w_release_date_plus_90d_rank).alias("rows_in_release_date_plus_90d_rank"),
        F.round(F.avg("duration_sec_cume_dist").over(w_release_date_plus_90d_rank), 2).alias("release_date_plus_90d_rank_avg_duration_sec_cume"),
        F.lead("duration_sec").over(w_artist_country_release_date_3).alias("next_duration_sec"),
    )
)

totals = (
    ranked_2
    .filter(F.col("duration_sec_vs_release_date_plus_90d_rank_avg") <= 168.0)
    .groupBy(
        "is_explicit",
        "duration_sec_cume_dist_rank",
        "release_date_plus_90d_month_min_duration_sec_den",
    )
    .agg(
        F.percentile_approx("release_date_plus_90d_rank_avg_duration_sec_cume", 0.5).alias("median_release_date_plus_90d_rank_avg_duration_s"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_not_is_explicit"),
        F.sum("duration_sec").alias("total_duration_sec"),
        F.round(F.sum("duration_sec_vs_artist_id_avg"), 2).alias("total_duration_sec_vs_artist_id_avg"),
        F.max("duration_sec_cume_dist_percent_rank").alias("max_duration_sec_cume_dist_percent_rank_check"),
    )
    .filter(F.col("max_duration_sec_cume_dist_percent_rank_check") > 0.1)
    .drop("max_duration_sec_cume_dist_percent_rank_check")
)

sequenced = (
    totals
    .select(
        "is_explicit",
        "duration_sec_cume_dist_rank",
        "release_date_plus_90d_month_min_duration_sec_den",
        "median_release_date_plus_90d_rank_avg_duration_s",
        "distinct_ids",
        "n_not_is_explicit",
        "total_duration_sec",
        F.last("total_duration_sec", ignorenulls=True).over(w_is_explicit_by_n_not_is_expl).alias("last_known_total_duration_sec"),
        F.max("distinct_ids").over(w_is_explicit_by_n_not_is_expl).alias("max_distinct_ids_so_far"),
        F.max("n_not_is_explicit").over(w_is_explicit_by_n_not_is_expl).alias("max_n_not_is_explicit_so_far"),
    )
)

result = (
    sequenced
    .select(
        "is_explicit",
        "duration_sec_cume_dist_rank",
        "median_release_date_plus_90d_rank_avg_duration_s",
        "distinct_ids",
        "total_duration_sec",
        "last_known_total_duration_sec",
        "max_distinct_ids_so_far",
        "max_n_not_is_explicit_so_far",
        F.greatest(F.col("max_distinct_ids_so_far"), F.col("distinct_ids")).alias("greatest_max_distinct_ids_so_far_distinct_ids"),
        F.round(F.col("max_distinct_ids_so_far") / F.nullif(F.col("total_duration_sec"), F.lit(0)), 3).alias("max_distinct_ids_so_far_per_total_duration_sec"),
    )
    .orderBy(F.col("max_distinct_ids_so_far").desc())
)
