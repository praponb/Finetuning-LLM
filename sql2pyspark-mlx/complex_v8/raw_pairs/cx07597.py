from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_source_played_at_plus_1d_to = Window.partitionBy("source", F.to_date("played_at_plus_1d")).orderBy("listener_birth_year", "id")
w_listener_id = Window.partitionBy("listener_id")
w_source_played_at_plus_1d_to_2 = Window.partitionBy("source", F.to_date("played_at_plus_1d")).orderBy("listener_birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_distinct_track_i = Window.partitionBy("listener_id", "distinct_track_id_in_is_skipped")
w_all_by_played_at_plus_1d_day = Window.orderBy(F.col("played_at_plus_1d_day").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_played_at_plus_1d_day_2 = Window.orderBy(F.col("played_at_plus_1d_day").asc_nulls_last())
w_distinct_track_id_in_listene = Window.partitionBy("distinct_track_id_in_listener_id")
w_all_by_played_at_plus_1d_day_3 = Window.orderBy(F.col("played_at_plus_1d_day").asc_nulls_last(), "id")
w_all_by_listener_birth_year_t = Window.orderBy(F.col("listener_birth_year_tercile").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_played_at_plus_1d = Window.partitionBy("is_skipped", "played_at_plus_1d_day_rank")
w_all_by_total_listener_birth = Window.orderBy(F.col("total_listener_birth_year_tercile").desc(), "is_skipped", "distinct_track_id_in_is_skipped", "played_at_plus_1d_day_rank")

valid_plays = (
    df_plays
    .filter((F.col("ms_played") >= 129336) & (F.col("track_id") <= 17))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 1 DAYS")).alias("played_at_plus_1d"),
    )
)

enriched = (
    valid_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .filter(F.length(F.trim(F.col("l.username"))) >= 8)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.played_at_plus_1d",
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("track_id").over(w_is_skipped)).alias("distinct_track_id_in_is_skipped"),
        F.ntile(3).over(w_source_played_at_plus_1d_to).alias("listener_birth_year_tercile"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_is_skipped), 2).alias("listener_birth_year_vs_is_skipped_avg"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.max("ms_played").over(w_listener_id).alias("listener_id_max_ms_played"),
        F.max("ms_played").over(w_source_played_at_plus_1d_to_2).alias("max_ms_played_so_far"),
        F.size(F.collect_set("track_id").over(w_listener_id)).alias("distinct_track_id_in_listener_id"),
    )
)

scored = (
    sequenced
    .select(
        "*",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.date_trunc("day", F.col("played_at_plus_1d")).alias("played_at_plus_1d_day"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "source",
        "played_at_plus_1d",
        "listener_username",
        "listener_birth_year",
        "distinct_track_id_in_is_skipped",
        "listener_birth_year_tercile",
        "listener_birth_year_vs_is_skipped_avg",
        "rows_in_is_skipped",
        "distinct_device_in_listener_id",
        "rows_in_listener_id",
        "listener_id_max_ms_played",
        "max_ms_played_so_far",
        "ms_played_bucket",
        "played_at_plus_1d_day",
        F.sum("listener_birth_year_tercile").over(w_listener_id_distinct_track_i).alias("listener_id_distinct_track_id_in_is_skipped_sum_"),
        F.round(F.avg("listener_id_max_ms_played").over(w_all_by_played_at_plus_1d_day), 2).alias("listener_id_max_ms_played_avg_3_prior"),
        F.rank().over(w_all_by_played_at_plus_1d_day_2).alias("played_at_plus_1d_day_rank"),
        F.round(F.col("ms_played_bucket") / F.nullif(F.sum("ms_played_bucket").over(w_distinct_track_id_in_listene), F.lit(0)), 4).alias("ms_played_bucket_share_of_distinct_track_id_in_l"),
        F.dense_rank().over(w_all_by_played_at_plus_1d_day_2).alias("played_at_plus_1d_day_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_played_at_plus_1d_day_3), 3).alias("played_at_plus_1d_day_percent_rank"),
        F.nth_value("listener_birth_year", 2).over(w_all_by_listener_birth_year_t).alias("second_listener_birth_year"),
        F.round(F.col("listener_birth_year") - F.avg("listener_birth_year").over(w_listener_id_distinct_track_i), 2).alias("listener_birth_year_vs_listener_id_distinct_trac"),
    )
)

rollup_base = (
    metrics
    .groupBy(
        "is_skipped",
        "distinct_track_id_in_is_skipped",
        "played_at_plus_1d_day_rank",
    )
    .agg(
        F.sum("listener_birth_year_tercile").alias("total_listener_birth_year_tercile"),
        F.max("ms_played_bucket").alias("max_ms_played_bucket"),
    )
)

metrics_2 = (
    rollup_base
    .select(
        "is_skipped",
        "distinct_track_id_in_is_skipped",
        "played_at_plus_1d_day_rank",
        "total_listener_birth_year_tercile",
        "max_ms_played_bucket",
        F.round(F.col("played_at_plus_1d_day_rank") - F.avg("played_at_plus_1d_day_rank").over(w_is_skipped_played_at_plus_1d), 2).alias("played_at_plus_1d_day_rank_vs_is_skipped_played_"),
        F.row_number().over(w_all_by_total_listener_birth).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_total_listener_birth), 3).alias("total_listener_birth_year_tercile_percent_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("rn").between(4, 6))
    .select(
        "is_skipped",
        "distinct_track_id_in_is_skipped",
        "played_at_plus_1d_day_rank",
        "total_listener_birth_year_tercile",
        "max_ms_played_bucket",
        "played_at_plus_1d_day_rank_vs_is_skipped_played_",
        "rn",
        "total_listener_birth_year_tercile_percent_rank",
    )
)
