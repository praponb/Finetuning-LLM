from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_track_id_by_track_bpm = Window.partitionBy("device", "track_id").orderBy("track_bpm", "id").rowsBetween(-6, -1)
w_track_id_ms_played_tier_by_t = Window.partitionBy("track_id", "ms_played_tier").orderBy("track_bpm")
w_source_ms_played_tier = Window.partitionBy("source", "ms_played_tier")
w_device_track_id_by_track_bpm_2 = Window.partitionBy("device", "track_id").orderBy("track_bpm")
w_device_track_id_by_track_bpm_3 = Window.partitionBy("device", "track_id").orderBy("track_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_distinct_devi = Window.partitionBy("listener_id").orderBy(F.col("distinct_device_in_source_ms_played_tier").desc(), "id")
w_is_skipped_source = Window.partitionBy("is_skipped", "source")
w_is_skipped_distinct_device_i = Window.partitionBy("is_skipped", "distinct_device_in_source_ms_played_tier").orderBy(F.col("track_bpm").asc_nulls_last(), "id")

filtered_plays = (
    df_plays
    .filter(F.col("track_id") >= 11)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 246779, "HIGH").when(F.col("ms_played") >= 73177, "MEDIUM").otherwise("LOW").alias("ms_played_tier"),
    )
)

combined_details = (
    filtered_plays.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.title").alias("track_title"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.max("track_bpm").over(w_device_track_id_by_track_bpm).alias("track_bpm_max_6_prior"),
        F.dense_rank().over(w_track_id_ms_played_tier_by_t).alias("track_bpm_dense_rank"),
        F.min("track_bpm").over(w_source_ms_played_tier).alias("source_ms_played_tier_min_track_bpm"),
        F.round(F.cume_dist().over(w_device_track_id_by_track_bpm_2), 3).alias("track_bpm_cume_dist"),
        F.first("track_bpm").over(w_device_track_id_by_track_bpm_3).alias("first_track_bpm"),
        F.size(F.collect_set("device").over(w_source_ms_played_tier)).alias("distinct_device_in_source_ms_played_tier"),
        F.size(F.collect_set("track_id").over(w_source_ms_played_tier)).alias("distinct_track_id_in_source_ms_played_tier"),
        F.count(F.lit(1)).over(w_source_ms_played_tier).alias("rows_in_source_ms_played_tier"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.ntile(5).over(w_listener_id_by_distinct_devi).alias("distinct_device_in_source_ms_played_tier_quintil"),
        F.round(F.col("track_bpm_cume_dist") - F.avg("track_bpm_cume_dist").over(w_is_skipped_source), 2).alias("track_bpm_cume_dist_vs_is_skipped_source_avg"),
        F.lag("track_bpm_max_6_prior", 1, 0).over(w_is_skipped_distinct_device_i).alias("prev_track_bpm_max_6_prior"),
        F.round(F.percent_rank().over(w_listener_id_by_distinct_devi), 3).alias("distinct_device_in_source_ms_played_tier_percent"),
        F.round(F.avg("track_bpm").over(w_is_skipped_source), 2).alias("is_skipped_source_avg_track_bpm"),
        F.size(F.collect_set("track_id").over(w_is_skipped_source)).alias("distinct_track_id_in_is_skipped_source"),
        F.min("first_track_bpm").over(w_is_skipped_source).alias("is_skipped_source_min_first_track_bpm"),
    )
)

result = (
    ranked
    .filter(F.col("source_ms_played_tier_min_track_bpm") >= 582)
    .select(
        "id",
        "device",
        "source",
        "ms_played_tier",
        "track_bpm",
        "track_title",
        "track_bpm_max_6_prior",
        "track_bpm_dense_rank",
        "source_ms_played_tier_min_track_bpm",
        "first_track_bpm",
        "distinct_device_in_source_ms_played_tier",
        "rows_in_source_ms_played_tier",
        "prev_track_bpm_max_6_prior",
        "distinct_device_in_source_ms_played_tier_percent",
        "distinct_track_id_in_is_skipped_source",
        "is_skipped_source_min_first_track_bpm",
        F.when(F.col("track_bpm_max_6_prior") >= 372, "HIGH").when(F.col("track_bpm_max_6_prior") >= 228, "MEDIUM").otherwise("LOW").alias("track_bpm_max_6_prior_tier"),
    )
)
