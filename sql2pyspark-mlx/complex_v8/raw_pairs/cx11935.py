from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_prior_6 = Window.orderBy("ms_played", "id").rowsBetween(-6, -1)
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_source = Window.partitionBy("source")
w_source_listener_id_by_ms_pla = Window.partitionBy("source", "listener_id").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_device_by_ms_played = Window.partitionBy("track_id", "device").orderBy(F.col("ms_played").desc(), "id")
w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy(F.col("ms_played").desc(), "id")
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_listener_id_by_ms_played_den = Window.partitionBy("listener_id").orderBy(F.col("ms_played_dense_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device = Window.partitionBy("device")
w_distinct_listener_id_in_sour = Window.partitionBy("distinct_listener_id_in_source")
w_ms_played_dense_rank_2_ms_pl = Window.partitionBy("ms_played_dense_rank_2", "ms_played_dense_rank_3").orderBy("ms_played_dense_rank_2")
w_listener_id_by_ms_played_den_2 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_dense_rank_2").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_plays
    .filter(F.col("source") != "album")
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "device_slug",
        F.round(F.avg("ms_played").over(w_all_by_ms_played_prior_6), 2).alias("ms_played_avg_6_prior"),
        F.dense_rank().over(w_all_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.size(F.collect_set("listener_id").over(w_source)).alias("distinct_listener_id_in_source"),
        F.last("ms_played").over(w_source_listener_id_by_ms_pla).alias("last_ms_played"),
        F.lag("ms_played").over(w_track_id_device_by_ms_played).alias("prev_ms_played"),
        F.dense_rank().over(w_listener_id_by_ms_played).alias("ms_played_dense_rank_2"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank_3"),
        F.min("ms_played").over(w_source).alias("source_min_ms_played"),
    )
)

leaders = (
    with_windows
    .filter((F.col("ms_played_dense_rank_2") <= 3) & (F.col("source_min_ms_played") > 1603000))
)

metrics = (
    leaders
    .select(
        "id",
        "ms_played",
        "device_slug",
        "ms_played_avg_6_prior",
        "ms_played_dense_rank",
        "last_ms_played",
        "ms_played_dense_rank_2",
        "ms_played_dense_rank_3",
        "source_min_ms_played",
        F.first("ms_played", ignorenulls=True).over(w_listener_id_by_ms_played_den).alias("first_known_ms_played"),
        F.sum("prev_ms_played").over(w_device).alias("device_sum_prev_ms_played"),
        F.size(F.collect_set("device").over(w_distinct_listener_id_in_sour)).alias("distinct_device_in_distinct_listener_id_in_sourc"),
        F.round(F.col("ms_played_avg_6_prior") - F.avg("ms_played_avg_6_prior").over(w_device), 2).alias("ms_played_avg_6_prior_vs_device_avg"),
        F.round(F.cume_dist().over(w_ms_played_dense_rank_2_ms_pl), 3).alias("ms_played_dense_rank_2_cume_dist"),
        F.nth_value("ms_played", 2).over(w_listener_id_by_ms_played_den_2).alias("second_ms_played"),
        F.round(F.percent_rank().over(w_ms_played_dense_rank_2_ms_pl), 3).alias("ms_played_dense_rank_2_percent_rank"),
        F.sum("ms_played_dense_rank_3").over(w_distinct_listener_id_in_sour).alias("distinct_listener_id_in_source_sum_ms_played_den"),
    )
)

result = (
    metrics
    .select(
        "id",
        "ms_played",
        "device_slug",
        "ms_played_avg_6_prior",
        "last_ms_played",
        "ms_played_dense_rank_2",
        "ms_played_dense_rank_3",
        "source_min_ms_played",
        "first_known_ms_played",
        "second_ms_played",
        "distinct_listener_id_in_source_sum_ms_played_den",
    )
)
