from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_plus_12m_year = Window.partitionBy(F.year("release_date_plus_12m"))
w_release_date_plus_12m_month = Window.partitionBy(F.date_trunc("month", "release_date_plus_12m")).orderBy("duration_sec", "id")
w_release_date_plus_12m_month_2 = Window.partitionBy(F.date_trunc("month", "release_date_plus_12m")).orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_first_duration_sec_tier_dist = Window.partitionBy("first_duration_sec_tier", "distinct_artist_id_in_release_date_plus_12m_year")
w_all_by_artist_id_min_duratio = Window.orderBy(F.col("artist_id_min_duration_sec").desc(), "id")
w_all_by_release_date = Window.orderBy(F.col("release_date").asc_nulls_last(), "id")
w_all_by_release_date_running = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.length(F.trim(F.col("title"))) >= 4))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

metrics = (
    cleaned_tracks
    .select(
        "*",
        F.count(F.lit(1)).over(w_release_date_plus_12m_year).alias("rows_in_release_date_plus_12m_year"),
        F.ntile(4).over(w_release_date_plus_12m_month).alias("duration_sec_quartile"),
        F.round(F.avg("bpm").over(w_release_date_plus_12m_month_2), 2).alias("bpm_avg_2_prior"),
        F.first("duration_sec").over(w_artist_id_by_release_date_fu).alias("first_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_release_date_plus_12m_year)).alias("distinct_artist_id_in_release_date_plus_12m_year"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.ntile(10).over(w_release_date_plus_12m_month).alias("duration_sec_decile"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.datediff(F.col("release_date"), F.col("release_date_plus_12m")).alias("days_release_date_plus_12m_to_release_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.when(F.col("first_duration_sec") >= 312, "large").when(F.col("first_duration_sec") >= 183, "medium").otherwise("small").alias("first_duration_sec_tier"),
    )
)

ranked = (
    derived
    .select(
        "id",
        "duration_sec",
        "release_date",
        "moods",
        "release_date_plus_12m",
        "duration_sec_quartile",
        "distinct_artist_id_in_release_date_plus_12m_year",
        "duration_sec_decile",
        "days_release_date_plus_12m_to_release_date",
        "days_since_release_date",
        "first_duration_sec_tier",
        F.round(F.col("duration_sec_decile") / F.nullif(F.sum("duration_sec_decile").over(w_first_duration_sec_tier_dist), F.lit(0)), 4).alias("duration_sec_decile_share_of_first_duration_sec_"),
        (F.col("duration_sec_decile") - F.lag("duration_sec_decile").over(w_all_by_artist_id_min_duratio)).alias("duration_sec_decile_change"),
        F.lead("bpm", 2, 0).over(w_all_by_artist_id_min_duratio).alias("next2_bpm"),
        F.rank().over(w_all_by_artist_id_min_duratio).alias("artist_id_min_duration_sec_rank"),
        F.round(F.avg("first_duration_sec").over(w_first_duration_sec_tier_dist), 2).alias("first_duration_sec_tier_distinct_artist_id_in_re"),
        F.rank().over(w_all_by_release_date).alias("release_date_rank"),
        F.last("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_release_date"),
        F.round(F.avg("duration_sec").over(w_first_duration_sec_tier_dist), 2).alias("first_duration_sec_tier_distinct_artist_id_in_re_2"),
    )
)

summary = (
    ranked
    .filter(F.array_contains(F.col("moods"), "chill"))
    .groupBy(
        F.date_format("last_known_release_date", "yyyy-MM").alias("last_known_release_date_ym"),
        "duration_sec_quartile",
        "distinct_artist_id_in_release_date_plus_12m_year",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.stddev("first_duration_sec_tier_distinct_artist_id_in_re_2"), 2).alias("stddev_first_duration_sec_tier_distinct_artist_i"),
        F.countDistinct("first_duration_sec_tier").alias("distinct_first_duration_sec_tiers"),
        F.min("release_date_plus_12m").alias("first_release_date_plus_12m"),
        F.sum("days_since_release_date").alias("total_days_since_release_date"),
        F.sum("release_date_rank").alias("total_release_date_rank"),
    )
)

result = (
    summary
    .select(
        "last_known_release_date_ym",
        "duration_sec_quartile",
        "distinct_artist_id_in_release_date_plus_12m_year",
        "row_count",
        "stddev_first_duration_sec_tier_distinct_artist_i",
        "distinct_first_duration_sec_tiers",
        "first_release_date_plus_12m",
        "total_days_since_release_date",
        F.coalesce(F.col("total_release_date_rank"), F.lit(0)).alias("total_release_date_rank_filled"),
        F.least(F.col("duration_sec_quartile"), F.col("total_release_date_rank")).alias("least_duration_sec_quartile_total_release_date_r"),
    )
    .orderBy("stddev_first_duration_sec_tier_distinct_artist_i")
)
