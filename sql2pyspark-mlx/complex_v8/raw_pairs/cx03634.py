from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_row_count_by_min_duration_se = Window.partitionBy("row_count").orderBy("min_duration_sec")
w_artist_id_distinct_artist_id = Window.partitionBy("artist_id", "distinct_artist_ids")
w_row_count_by_min_duration_se_2 = Window.partitionBy("row_count").orderBy("min_duration_sec", "artist_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_min_duration_sec_rank_second = Window.partitionBy("min_duration_sec_rank", "second_row_count").orderBy("min_duration_sec", "artist_id")
w_last_track_release_date_year = Window.partitionBy(F.year("last_track_release_date"))

cleaned = (
    df_tracks
    .filter(F.col("release_date").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.when(F.col("duration_sec") >= 352, "A").when(F.col("duration_sec") >= 209, "B").otherwise("C").alias("duration_sec_tier"),
    )
)

stats = (
    cleaned
    .groupBy("artist_id")
    .agg(
        F.min("duration_sec").alias("min_duration_sec"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.sum(F.when(F.col("is_explicit"), F.col("duration_sec"))).alias("duration_sec_is_explicit_true"),
        F.count("*").alias("row_count"),
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
    )
)

with_trends = (
    stats
    .select(
        "*",
        F.rank().over(w_row_count_by_min_duration_se).alias("min_duration_sec_rank"),
        F.round(F.col("distinct_artist_ids") / F.nullif(F.sum("distinct_artist_ids").over(w_artist_id_distinct_artist_id), F.lit(0)), 4).alias("distinct_artist_ids_share_of_artist_id_distinct_"),
        F.nth_value("distinct_artist_ids", 2).over(w_row_count_by_min_duration_se_2).alias("second_distinct_artist_ids"),
        F.nth_value("row_count", 2).over(w_row_count_by_min_duration_se_2).alias("second_row_count"),
        F.round(F.col("avg_duration_sec") - F.avg("avg_duration_sec").over(w_artist_id_distinct_artist_id), 2).alias("avg_duration_sec_vs_artist_id_distinct_artist_id"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.max("release_date").alias("last_track_release_date"))
)

profiled = (
    with_trends.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.last_track_release_date")
)

qualified = (
    profiled.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date") < F.lit("2024-12-22").cast("date")), "left_semi")
)

sequenced = (
    qualified
    .withColumn("next2_row_count", F.lead("row_count", 2).over(w_min_duration_sec_rank_second))
    .withColumn("last_track_release_date_year_sum_min_duration_se", F.sum("min_duration_sec").over(w_last_track_release_date_year))
)

annotated = (
    sequenced
    .filter(F.col("min_duration_sec_rank") <= 8)
    .select(
        "artist_id",
        "min_duration_sec",
        "distinct_artist_ids",
        "row_count",
        "avg_duration_sec",
        "second_distinct_artist_ids",
        "avg_duration_sec_vs_artist_id_distinct_artist_id",
        "last_track_release_date",
        "last_track_release_date_year_sum_min_duration_se",
        F.round(F.col("avg_duration_sec") / F.nullif(F.col("distinct_artist_ids"), F.lit(0)), 3).alias("avg_duration_sec_per_distinct_artist_ids"),
        F.coalesce(F.col("min_duration_sec"), F.lit(0)).alias("min_duration_sec_filled"),
        (F.floor(F.col("second_row_count") / 2) * 2).alias("second_row_count_bucket"),
    )
)

result = (
    annotated
    .select(
        "artist_id",
        "min_duration_sec",
        "distinct_artist_ids",
        "row_count",
        "avg_duration_sec",
        "avg_duration_sec_vs_artist_id_distinct_artist_id",
        "last_track_release_date",
        "last_track_release_date_year_sum_min_duration_se",
        "avg_duration_sec_per_distinct_artist_ids",
        "min_duration_sec_filled",
        F.least(F.col("row_count"), F.col("distinct_artist_ids")).alias("least_row_count_distinct_artist_ids"),
        F.least(F.col("min_duration_sec_filled"), F.col("avg_duration_sec")).alias("least_min_duration_sec_filled_avg_duration_sec"),
    )
    .orderBy("last_track_release_date_year_sum_min_duration_se", "artist_id")
    .limit(10)
)
