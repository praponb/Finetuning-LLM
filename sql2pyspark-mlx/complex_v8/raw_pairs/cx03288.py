from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_played_at_month_source_group = Window.partitionBy(F.month("played_at"), "source_group").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_all_by_played_at_running = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_source_group = Window.partitionBy("track_id", "source_group")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")
w_all_by_played_at_full = Window.orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_distinct_device_in_li = Window.orderBy("distinct_device_in_listener_id_device_in_phone_d", "second_played_at_year", "distinct_device_in_listener_id", "device").rowsBetween(-2, Window.currentRow)
w_all_by_distinct_device_in_li_2 = Window.orderBy("distinct_device_in_listener_id_device_in_phone_d", "second_played_at_year", "distinct_device_in_listener_id", "device")

cleaned_plays = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("source").isin("radio", "playlist"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
)

sequenced = (
    cleaned_plays
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source_group",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id), F.lit(0)), 4).alias("ms_played_share_of_listener_id"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.sum("ms_played").over(w_played_at_month_source_group).alias("ms_played_sum_3_prior"),
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        F.count(F.lit(1)).over(w_all_by_played_at_running).alias("running_count"),
        F.sum("ms_played").over(w_listener_id).alias("listener_id_sum_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_track_id_source_group), 2).alias("ms_played_vs_track_id_source_group_avg"),
        F.dense_rank().over(w_all_by_ms_played).alias("ms_played_dense_rank"),
        F.round(F.avg("ms_played").over(w_listener_id), 2).alias("listener_id_avg_ms_played"),
        F.lead("played_at").over(w_all_by_ms_played).alias("next_played_at"),
        F.min("ms_played").over(w_track_id_source_group).alias("track_id_source_group_min_ms_played"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at"),
    )
)

totals = (
    sequenced
    .groupBy(
        F.year("second_played_at").alias("second_played_at_year"),
        "distinct_device_in_listener_id",
        "device",
    )
    .agg(
        F.count(F.when(F.col("device").isin("desktop", "speaker", "phone"), 1)).alias("n_device_in_desktop_speaker_ph"),
        F.count("*").alias("n_rows"),
        F.round(F.sum("ms_played_dense_rank") / F.nullif(F.sum("track_id_source_group_min_ms_played"), F.lit(0)), 3).alias("ms_played_dense_rank_to_track_id_source_group_mi"),
        F.sum(F.when(F.col("device").isin("phone", "desktop"), F.col("distinct_device_in_listener_id")).otherwise(0)).alias("distinct_device_in_listener_id_device_in_phone_d"),
        F.round(F.sum("listener_id_min_ms_played") / F.nullif(F.sum("distinct_device_in_listener_id"), F.lit(0)), 3).alias("listener_id_min_ms_played_to_distinct_device_in_"),
        F.max("ms_played_dense_rank").alias("max_ms_played_dense_rank_check"),
    )
    .filter(F.col("max_ms_played_dense_rank_check") > 2)
    .drop("max_ms_played_dense_rank_check")
)

sequenced_2 = (
    totals
    .select(
        "second_played_at_year",
        "distinct_device_in_listener_id",
        "device",
        "n_device_in_desktop_speaker_ph",
        "n_rows",
        "distinct_device_in_listener_id_device_in_phone_d",
        "listener_id_min_ms_played_to_distinct_device_in_",
        F.max("n_rows").over(w_all_by_distinct_device_in_li).alias("n_rows_max_3"),
        F.row_number().over(w_all_by_distinct_device_in_li_2).alias("distinct_device_in_listener_id_device_in_phone_d_2"),
    )
)

result = (
    sequenced_2
    .select(
        "second_played_at_year",
        "distinct_device_in_listener_id",
        "n_device_in_desktop_speaker_ph",
        "n_rows",
        "distinct_device_in_listener_id_device_in_phone_d",
        "listener_id_min_ms_played_to_distinct_device_in_",
        "distinct_device_in_listener_id_device_in_phone_d_2",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy(
        "device_len",
        F.col("listener_id_min_ms_played_to_distinct_device_in_").desc(),
    )
)
