from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy("ms_played_distance", "id")
w_is_skipped_track_id = Window.partitionBy("is_skipped", "track_id")
w_listener_id_by_ms_played_dis = Window.partitionBy("listener_id").orderBy("ms_played_distance_vs_is_skipped_track_id_avg", "id")
w_ms_played_distance_rank_sour = Window.partitionBy("ms_played_distance_rank", "source").orderBy(F.col("ms_played_distance_vs_is_skipped_track_id_avg").desc(), "id")
w_ms_played_distance_rank_devi = Window.partitionBy("ms_played_distance_rank", "device").orderBy(F.col("ms_played_distance_rank").desc_nulls_first(), "id")
w_latest = Window.partitionBy("listener_id").orderBy("ms_played_distance_rank", "id")
w_ms_played_distance_rank_by_m = Window.partitionBy("ms_played_distance_rank").orderBy("ms_played_distance", "id").rowsBetween(-1, -1)
w_ms_played_distance_rank_by_m_2 = Window.partitionBy("ms_played_distance_rank").orderBy("ms_played_distance", "id")
w_is_skipped_source = Window.partitionBy("is_skipped", "source")

cleaned = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 225733).alias("ms_played_distance"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

windowed = (
    cleaned
    .withColumn("ms_played_distance_rank", F.rank().over(w_source_is_skipped_by_ms_play))
    .withColumn("ms_played_distance_vs_is_skipped_track_id_avg", F.round(F.col("ms_played_distance") - F.avg("ms_played_distance").over(w_is_skipped_track_id), 2))
)

metrics = (
    windowed
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "ms_played_distance",
        "ms_played_distance_rank",
        "ms_played_distance_vs_is_skipped_track_id_avg",
        F.row_number().over(w_listener_id_by_ms_played_dis).alias("ms_played_distance_vs_is_skipped_track_id_avg_se"),
        F.lag("ms_played_distance_rank").over(w_listener_id_by_ms_played_dis).alias("prev_ms_played_distance_rank"),
        (F.col("ms_played_distance_rank") - F.lag("ms_played_distance_rank").over(w_ms_played_distance_rank_sour)).alias("ms_played_distance_rank_change"),
        (F.col("ms_played_distance") - F.lag("ms_played_distance").over(w_ms_played_distance_rank_sour)).alias("ms_played_distance_change"),
        F.round(F.cume_dist().over(w_ms_played_distance_rank_devi), 3).alias("ms_played_distance_rank_cume_dist"),
    )
)

device_stats = (
    metrics
    .groupBy("device")
    .agg(F.avg("ms_played_distance_vs_is_skipped_track_id_avg_se").alias("avg_ms_played_distance_vs_is_skipped_track_id_av"))
)

vs_group = (
    metrics.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played_distance_vs_is_skipped_track_id_avg_se") > F.col("g.avg_ms_played_distance_vs_is_skipped_track_id_av"))
    .select(
        "s.*",
        "g.avg_ms_played_distance_vs_is_skipped_track_id_av",
        F.round(F.col("s.ms_played_distance_vs_is_skipped_track_id_avg_se") - F.col("g.avg_ms_played_distance_vs_is_skipped_track_id_av"), 2).alias("ms_played_distance_vs_is_skipped_track_id_avg_se_2"),
    )
)

unique_rows = (
    vs_group
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    unique_rows
    .select(
        "*",
        F.sum("prev_ms_played_distance_rank").over(w_ms_played_distance_rank_by_m).alias("prev_ms_played_distance_rank_sum_1_prior"),
        F.lag("ms_played_distance", 3).over(w_ms_played_distance_rank_by_m_2).alias("prev3_ms_played_distance"),
        F.count(F.lit(1)).over(w_is_skipped_source).alias("rows_in_is_skipped_source"),
        F.row_number().over(w_ms_played_distance_rank_by_m_2).alias("ms_played_distance_seq"),
    )
)

per_group = (
    with_windows
    .filter(F.col("ms_played_distance_vs_is_skipped_track_id_avg_se_2") > -3.0)
    .groupBy(
        "ms_played_distance_vs_is_skipped_track_id_avg_se",
        "is_skipped",
        "device",
    )
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.sum(F.when(~F.col("is_skipped"), F.col("ms_played_distance_rank")).otherwise(0)).alias("ms_played_distance_rank_is_skipped_false"),
    )
)

result = (
    per_group
    .filter(F.size(F.col("source_list")) >= 1)
    .select(
        "ms_played_distance_vs_is_skipped_track_id_avg_se",
        "is_skipped",
        "source_list",
        "ms_played_distance_rank_is_skipped_false",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
    )
    .orderBy(F.col("is_skipped_label").asc())
)
