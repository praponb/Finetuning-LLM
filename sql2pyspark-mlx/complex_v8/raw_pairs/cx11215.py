from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").desc(), F.col("id").desc())
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_track_id_by_listener_signup = Window.partitionBy("track_id").orderBy("listener_signup_date")
w_track_id_by_listener_signup_2 = Window.partitionBy("track_id").orderBy("listener_signup_date", "id")
w_track_id_listener_id_by_list = Window.partitionBy("track_id", "listener_id").orderBy("listener_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_plays = (
    df_plays
    .filter(F.col("source") != "playlist")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

latest = (
    cleaned_plays
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details_listener = (
    latest.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.ms_played",
        "s.device_slug",
        F.col("l.signup_date").alias("listener_signup_date"),
        F.col("l.plan").alias("listener_plan"),
        F.col("t.bpm").alias("track_bpm"),
    )
)

with_trends = (
    combined_details_listener
    .select(
        "*",
        F.sum("ms_played").over(w_track_id_listener_id).alias("track_id_listener_id_sum_ms_played"),
        F.round(F.percent_rank().over(w_track_id_by_listener_signup), 3).alias("listener_signup_date_percent_rank"),
        F.lag("played_at", 2).over(w_track_id_by_listener_signup_2).alias("prev2_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_listener_id), 2).alias("ms_played_vs_track_id_listener_id_avg"),
        F.min("track_bpm").over(w_track_id_listener_id_by_list).alias("min_track_bpm_so_far"),
        F.round(F.col("track_bpm") - F.avg("track_bpm").over(w_track_id_listener_id), 2).alias("track_bpm_vs_track_id_listener_id_avg"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(
        F.count(F.when(F.col("source").isNotNull(), 1)).alias("play_source_count"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_source_count"), F.lit(0)).alias("play_source_count"),
    )
)

summary = (
    profiled
    .groupBy("play_source_count", F.year("played_at").alias("played_at_year"))
    .agg(
        F.max("track_bpm_vs_track_id_listener_id_avg").alias("max_track_bpm_vs_track_id_listener_id_avg"),
        F.percentile_approx("track_id_listener_id_sum_ms_played", 0.5).alias("median_track_id_listener_id_sum_ms_played"),
        F.percentile_approx("track_bpm_vs_track_id_listener_id_avg", 0.5).alias("median_track_bpm_vs_track_id_listener_id_avg"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("track_bpm_vs_track_id_listener_id_avg") / F.nullif(F.sum("track_bpm"), F.lit(0)), 3).alias("track_bpm_vs_track_id_listener_id_avg_to_track_b"),
        F.count(F.when(F.col("listener_plan").isin("premium", "student"), 1)).alias("n_listener_plan_in_premium_stu"),
    )
)

result = (
    summary
    .select(
        "play_source_count",
        "played_at_year",
        "median_track_id_listener_id_sum_ms_played",
        "distinct_ids",
        F.coalesce(F.col("distinct_ids"), F.lit(0)).alias("distinct_ids_filled"),
        F.abs(F.col("play_source_count") - 2).alias("play_source_count_distance"),
    )
)
