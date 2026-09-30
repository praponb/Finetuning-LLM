from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_bpm = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id")
w_release_date_month_by_bpm_ru = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id")
w_is_explicit_by_release_date_2 = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(-2, Window.currentRow)
w_dedup = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), F.col("id").desc())
w_play_count_by_bpm_percent_ra = Window.partitionBy("play_count").orderBy("bpm_percent_rank", "id").rowsBetween(-2, -1)
w_release_date_month = Window.partitionBy(F.month("release_date"))
w_play_is_skipped_count_by_pla = Window.partitionBy("play_is_skipped_count").orderBy(F.col("play_count").desc(), "id")
w_play_count_by_bpm_percent_ra_2 = Window.partitionBy("play_count").orderBy("bpm_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_n_length_trim_t = Window.partitionBy("distinct_ids", "n_length_trim_title_7")
w_n_lower_title_like_ha_n_leng = Window.partitionBy("n_lower_title_like_ha", "n_length_trim_title_7").orderBy("total_release_date_month_min_bpm", "is_explicit").rowsBetween(-6, -1)

filtered_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

windowed = (
    filtered_tracks
    .select(
        "*",
        F.round(F.percent_rank().over(w_release_date_month_by_bpm), 3).alias("bpm_percent_rank"),
        F.last("bpm", ignorenulls=True).over(w_release_date_month_by_bpm_ru).alias("last_known_bpm"),
        F.lead("bpm", 2).over(w_is_explicit_by_release_date).alias("next2_bpm"),
        F.row_number().over(w_release_date_month_by_bpm).alias("bpm_seq"),
        F.max("bpm").over(w_is_explicit_by_release_date_2).alias("bpm_max_3"),
    )
)

latest = (
    windowed
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

metrics = (
    with_counts
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm_percent_rank",
        "last_known_bpm",
        "bpm_seq",
        "bpm_max_3",
        "play_is_skipped_count",
        "first_played_at",
        "play_count",
        F.max("bpm_percent_rank").over(w_play_count_by_bpm_percent_ra).alias("bpm_percent_rank_max_2_prior"),
        F.min("bpm").over(w_release_date_month).alias("release_date_month_min_bpm"),
        F.round(F.percent_rank().over(w_play_is_skipped_count_by_pla), 3).alias("play_count_percent_rank"),
        F.sum("bpm_max_3").over(w_release_date_month).alias("release_date_month_sum_bpm_max_3"),
        F.min("last_known_bpm").over(w_play_count_by_bpm_percent_ra_2).alias("min_last_known_bpm_so_far"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
    )
)

summary = (
    metrics
    .groupBy("is_explicit")
    .agg(
        F.sum(F.when(F.length(F.trim(F.col("title"))) >= 7, 1).otherwise(0)).alias("n_length_trim_title_7"),
        F.count(F.when(F.lower(F.col("title")).like("%ha%"), 1)).alias("n_lower_title_like_ha"),
        F.sum("release_date_month_min_bpm").alias("total_release_date_month_min_bpm"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("title"))).like("G%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_title_like_g"),
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("release_date_month_sum_bpm_max_3").alias("avg_release_date_month_sum_bpm_max_3_check"),
    )
    .filter(F.col("avg_release_date_month_sum_bpm_max_3_check") > 1128)
    .drop("avg_release_date_month_sum_bpm_max_3_check")
)

sequenced = (
    summary
    .select(
        "*",
        F.round(F.col("n_lower_title_like_ha") - F.avg("n_lower_title_like_ha").over(w_distinct_ids_n_length_trim_t), 2).alias("n_lower_title_like_ha_vs_distinct_ids_n_length_t"),
        F.count(F.lit(1)).over(w_distinct_ids_n_length_trim_t).alias("rows_in_distinct_ids_n_length_trim_title_7"),
        F.max("distinct_ids").over(w_n_lower_title_like_ha_n_leng).alias("distinct_ids_max_6_prior"),
        F.round(F.col("distinct_ids") - F.avg("distinct_ids").over(w_distinct_ids_n_length_trim_t), 2).alias("distinct_ids_vs_distinct_ids_n_length_trim_title"),
    )
)

result = (
    sequenced
    .select(
        "is_explicit",
        "n_length_trim_title_7",
        "n_lower_title_like_ha",
        "pct_upper_trim_title_like_g",
        "distinct_ids",
        "rows_in_distinct_ids_n_length_trim_title_7",
        "distinct_ids_max_6_prior",
        "distinct_ids_vs_distinct_ids_n_length_trim_title",
    )
    .orderBy(F.col("pct_upper_trim_title_like_g").desc())
)
