from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_run = Window.partitionBy("listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_listener_id = Window.partitionBy("source", "listener_id")
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_listener_id_track_id_by_ms_p = Window.partitionBy("listener_id", "track_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_min_ms_played_so_far = Window.orderBy("min_ms_played_so_far", "id")
w_distinct_track_id_in_source = Window.partitionBy("distinct_track_id_in_source_listener_id", "is_skipped").orderBy(F.col("distinct_track_id_in_source_listener_id").desc(), "id")
w_all_by_min_ms_played_so_far_2 = Window.orderBy("min_ms_played_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ms_played_tier_track_id = Window.partitionBy("ms_played_tier", "track_id")
w_all_by_min_ms_played_so_far_3 = Window.orderBy("min_ms_played_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_tier_distinct_trac = Window.partitionBy("ms_played_tier", "distinct_track_id_in_source_listener_id").orderBy(F.col("max_ms_played_so_far").desc(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_ms_played_vs_source_l = Window.orderBy("ms_played_vs_source_listener_id_avg", "id")
w_distinct_track_id_in_source_2 = Window.partitionBy("distinct_track_id_in_source_listener_id", "is_skipped").orderBy(F.col("distinct_track_id_in_source_listener_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_plays
    .filter((F.col("ms_played") > 71476) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("ms_played") >= 282311, "A").when(F.col("ms_played") >= 151112, "B").otherwise("C").alias("ms_played_tier"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.min("ms_played").over(w_device_listener_id_by_ms_pla).alias("min_ms_played_so_far"),
        F.max("ms_played").over(w_listener_id_by_ms_played_run).alias("max_ms_played_so_far"),
        F.size(F.collect_set("track_id").over(w_source_listener_id)).alias("distinct_track_id_in_source_listener_id"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_source_listener_id), 2).alias("ms_played_vs_source_listener_id_avg"),
        F.round(F.percent_rank().over(w_all_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.dense_rank().over(w_listener_id_by_ms_played).alias("ms_played_dense_rank"),
        F.sum("ms_played").over(w_listener_id_by_ms_played_run).alias("running_ms_played"),
        F.count(F.lit(1)).over(w_device_listener_id_by_ms_pla).alias("running_count"),
        F.rank().over(w_listener_id_track_id_by_ms_p).alias("ms_played_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.lag("ms_played_dense_rank").over(w_all_by_min_ms_played_so_far).alias("prev_ms_played_dense_rank"),
        F.row_number().over(w_distinct_track_id_in_source).alias("distinct_track_id_in_source_listener_id_seq"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_min_ms_played_so_far_2).alias("first_known_ms_played"),
        F.size(F.collect_set("listener_id").over(w_ms_played_tier_track_id)).alias("distinct_listener_id_in_ms_played_tier_track_id"),
        F.nth_value("ms_played_percent_rank", 2).over(w_all_by_min_ms_played_so_far_3).alias("second_ms_played_percent_rank"),
        F.round(F.avg("min_ms_played_so_far").over(w_ms_played_tier_distinct_trac), 2).alias("min_ms_played_so_far_avg_7"),
        F.row_number().over(w_all_by_ms_played_vs_source_l).alias("seq_no"),
        F.nth_value("ms_played", 2).over(w_distinct_track_id_in_source_2).alias("second_ms_played"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "ms_played_tier",
        "min_ms_played_so_far",
        "max_ms_played_so_far",
        "distinct_track_id_in_source_listener_id",
        "ms_played_dense_rank",
        "running_ms_played",
        "running_count",
        "ms_played_rank",
        "prev_ms_played_dense_rank",
        "distinct_track_id_in_source_listener_id_seq",
        "first_known_ms_played",
        "distinct_listener_id_in_ms_played_tier_track_id",
        "second_ms_played_percent_rank",
        "seq_no",
        "second_ms_played",
    )
    .orderBy(F.col("running_count").desc(), "id")
    .limit(10)
)
