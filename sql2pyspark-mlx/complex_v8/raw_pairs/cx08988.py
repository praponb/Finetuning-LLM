from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_ms_played = Window.partitionBy("listener_id").orderBy("ms_played", "id")
w_track_id_device_by_ms_played = Window.partitionBy("track_id", "device").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_device = Window.partitionBy("track_id", "device")
w_track_id_device_by_ms_played_2 = Window.partitionBy("track_id", "device").orderBy("ms_played")
w_source = Window.partitionBy("source")
w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_track_id_device_by_ms_played_3 = Window.partitionBy("track_id", "device").orderBy("ms_played", "id")
w_listener_id_by_ms_played_tie = Window.partitionBy("listener_id").orderBy("ms_played")
w_device_by_ms_played = Window.partitionBy("device").orderBy(F.col("ms_played").desc(), "id")
w_source_prev3_played_at_month = Window.partitionBy("source", F.month("prev3_played_at")).orderBy("ms_played_cume_dist", "id").rowsBetween(-3, Window.currentRow)
w_track_id = Window.partitionBy("track_id")
w_device_ms_played_decile_by_m = Window.partitionBy("device", "ms_played_decile").orderBy("ms_played_cume_dist_2", "id")

cleaned = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.last("played_at", ignorenulls=True).over(w_track_id_device_by_ms_played).alias("last_known_played_at"),
        F.max("ms_played").over(w_track_id_device).alias("track_id_device_max_ms_played"),
        F.round(F.cume_dist().over(w_track_id_device_by_ms_played_2), 3).alias("ms_played_cume_dist"),
        F.count(F.lit(1)).over(w_source).alias("rows_in_source"),
        F.lag("played_at", 3).over(w_all_by_played_at).alias("prev3_played_at"),
        F.ntile(10).over(w_track_id_device_by_ms_played_3).alias("ms_played_decile"),
        F.size(F.collect_set("listener_id").over(w_track_id_device)).alias("distinct_listener_id_in_track_id_device"),
        F.round(F.percent_rank().over(w_track_id_device_by_ms_played_2), 3).alias("ms_played_percent_rank"),
        F.round(F.cume_dist().over(w_listener_id_by_ms_played_tie), 3).alias("ms_played_cume_dist_2"),
        F.row_number().over(w_track_id_device_by_ms_played_3).alias("seq_no"),
        F.lead("ms_played").over(w_device_by_ms_played).alias("next_ms_played"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.round(F.avg("next_ms_played").over(w_source_prev3_played_at_month), 2).alias("next_ms_played_avg_4"),
        F.sum("ms_played_decile").over(w_track_id).alias("track_id_sum_ms_played_decile"),
        F.round(F.percent_rank().over(w_device_ms_played_decile_by_m), 3).alias("ms_played_cume_dist_2_percent_rank"),
        F.row_number().over(w_device_ms_played_decile_by_m).alias("rn"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
    )
)

grouped = (
    ranked
    .groupBy(
        F.date_format("last_known_played_at", "yyyy-MM").alias("last_known_played_at_ym"),
        "seq_no",
    )
    .agg(
        F.max("last_known_played_at").alias("last_last_known_played_at"),
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("distinct_listener_id_in_track_id_device"), 2).alias("stddev_distinct_listener_id_in_track_id_device"),
        F.countDistinct("source").alias("distinct_sources"),
    )
)

result = (
    grouped
    .select(
        "last_known_played_at_ym",
        "seq_no",
        "last_last_known_played_at",
        "device_list",
        "cnt",
        "distinct_sources",
    )
)
