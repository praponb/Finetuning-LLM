from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_by_ms_played_ties = Window.partitionBy("source").orderBy(F.col("ms_played").desc())
w_source_or_unknown_track_id = Window.partitionBy("source_or_unknown", "track_id")
w_all_by_ms_played_full = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_is_skipped_by_ms = Window.partitionBy("listener_id", "is_skipped").orderBy("ms_played", "id")
w_all_by_ms_played_running = Window.orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_quartile = Window.orderBy(F.col("ms_played_quartile").desc(), "id")
w_all_by_distinct_listener_id = Window.orderBy("distinct_listener_id_in_source_or_unknown_track_", "id")

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_source_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_or_unknown_track_id), F.lit(0)), 4).alias("ms_played_share_of_source_or_unknown_track_id"),
        F.first("ms_played").over(w_all_by_ms_played_full).alias("first_ms_played"),
        F.ntile(4).over(w_listener_id_is_skipped_by_ms).alias("ms_played_quartile"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_ms_played"),
        F.size(F.collect_set("listener_id").over(w_source_or_unknown_track_id)).alias("distinct_listener_id_in_source_or_unknown_track_"),
        F.size(F.collect_set("source").over(w_source_or_unknown_track_id)).alias("distinct_source_in_source_or_unknown_track_id"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
    )
)

annotated = sequenced.withColumn("source_len", F.length(F.trim(F.col("source"))))

windowed = (
    annotated
    .select(
        "*",
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id), 2).alias("ms_played_vs_track_id_avg"),
        F.lead("ms_played_dense_rank", 3).over(w_all_by_ms_played_quartile).alias("next3_ms_played_dense_rank"),
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
        F.round(F.cume_dist().over(w_all_by_distinct_listener_id), 3).alias("distinct_listener_id_in_source_or_unknown_track__2"),
    )
)

result = (
    windowed
    .filter(F.col("first_ms_played") > 108979)
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "source_or_unknown",
        "ms_played_percent_rank",
        "first_ms_played",
        "ms_played_quartile",
        "distinct_listener_id_in_source_or_unknown_track_",
        "source_len",
        "ms_played_vs_track_id_avg",
        "next3_ms_played_dense_rank",
        "distinct_listener_id_in_source_or_unknown_track__2",
        F.when(F.col("ms_played_dense_rank") >= 8, "large").when(F.col("ms_played_dense_rank") >= 4, "medium").otherwise("small").alias("ms_played_dense_rank_tier"),
        F.lower(F.regexp_replace(F.trim(F.col("source_or_unknown")), " ", "_")).alias("source_or_unknown_slug"),
    )
    .orderBy(F.col("first_ms_played").desc(), "id")
    .limit(10)
)
