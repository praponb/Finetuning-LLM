from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played_moving_1 = Window.partitionBy("device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_device_by_ms_played_ties = Window.partitionBy("device").orderBy(F.col("ms_played").desc())
w_device_by_ms_played_prior_6 = Window.partitionBy("device").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, -1)
w_all_by_ms_played_avg_6_prior = Window.orderBy("ms_played_avg_6_prior", "id")
w_distinct_listener_id_in_trac = Window.partitionBy("distinct_listener_id_in_track_id")

cleaned = (
    df_plays
    .filter(F.col("device").isin("phone", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.length(F.trim(F.col("source"))).alias("source_len"),
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.max("source_len").over(w_device_by_ms_played_moving_1).alias("source_len_max_2"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.dense_rank().over(w_device_by_ms_played_ties).alias("ms_played_dense_rank"),
        F.round(F.avg("ms_played").over(w_device_by_ms_played_prior_6), 2).alias("ms_played_avg_6_prior"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "source_len",
        "source_len_max_2",
        "rows_in_track_id",
        "distinct_listener_id_in_track_id",
        "ms_played_dense_rank",
        "ms_played_avg_6_prior",
        F.lag("ms_played_avg_6_prior").over(w_all_by_ms_played_avg_6_prior).alias("prev_ms_played_avg_6_prior"),
        F.round(F.col("ms_played_avg_6_prior") - F.avg("ms_played_avg_6_prior").over(w_distinct_listener_id_in_trac), 2).alias("ms_played_avg_6_prior_vs_distinct_listener_id_in"),
        F.min("ms_played_avg_6_prior").over(w_distinct_listener_id_in_trac).alias("distinct_listener_id_in_track_id_min_ms_played_a"),
        F.min("ms_played_dense_rank").over(w_distinct_listener_id_in_trac).alias("distinct_listener_id_in_track_id_min_ms_played_d"),
        F.row_number().over(w_all_by_ms_played_avg_6_prior).alias("seq_no"),
    )
)

result = (
    windowed
    .groupBy("distinct_listener_id_in_track_id")
    .pivot("device", ["tv", "speaker", "desktop", "phone"])
    .agg(F.sum("ms_played_avg_6_prior_vs_distinct_listener_id_in"))
)
