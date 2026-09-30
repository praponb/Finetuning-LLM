from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_track_id_by_ms_played = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")
w_source_track_id_by_ms_played_2 = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_track_id_by_ms_played_3 = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_source_track_id_by_ms_played_4 = Window.partitionBy("source", "track_id").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_source = Window.partitionBy("source")
w_all_by_max_rows_in_listener = Window.orderBy(F.col("max_rows_in_listener_id").asc_nulls_last(), "source")
w_all_by_max_rows_in_listener_2 = Window.orderBy(F.col("max_rows_in_listener_id").asc_nulls_last(), "source").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.concat(F.col("source"), F.lit("#"), F.col("listener_id").cast("string")).alias("source_ref"),
    )
)

windowed = (
    valid_plays
    .select(
        "*",
        F.sum("ms_played").over(w_source_track_id_by_ms_played).alias("running_ms_played"),
        F.min("ms_played").over(w_listener_id).alias("listener_id_min_ms_played"),
        F.nth_value("ms_played", 2).over(w_source_track_id_by_ms_played_2).alias("second_ms_played"),
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.lag("ms_played", 2, 0).over(w_source_track_id_by_ms_played_3).alias("prev2_ms_played"),
        F.sum("ms_played").over(w_source_track_id_by_ms_played_4).alias("ms_played_sum_2"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.when(F.col("device").isin("car", "phone"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other").alias("device_group"),
        F.concat(F.col("device"), F.lit("#"), F.col("id").cast("string")).alias("device_ref"),
        F.greatest(F.col("ms_played_sum_2"), F.col("rows_in_listener_id")).alias("greatest_ms_played_sum_2_rows_in_listener_id"),
    )
)

summary = (
    annotated
    .groupBy("source")
    .agg(
        F.max("rows_in_listener_id").alias("max_rows_in_listener_id"),
        F.sum("ms_played_sum_2").alias("total_ms_played_sum_2"),
        F.round(F.avg("running_ms_played"), 2).alias("avg_running_ms_played"),
        F.round(F.sum("listener_id_min_ms_played") / F.nullif(F.sum("second_ms_played"), F.lit(0)), 3).alias("listener_id_min_ms_played_to_second_ms_played_ra"),
        F.sum(F.when(~F.col("device").isin("phone", "speaker"), F.col("ms_played_sum_2"))).alias("ms_played_sum_2_device_not_in_phone_speaker"),
        F.max("ms_played").alias("max_ms_played_check"),
    )
    .filter(F.col("max_ms_played_check") > 56858)
    .drop("max_ms_played_check")
)

sequenced = (
    summary
    .select(
        "source",
        "max_rows_in_listener_id",
        "total_ms_played_sum_2",
        "avg_running_ms_played",
        "listener_id_min_ms_played_to_second_ms_played_ra",
        "ms_played_sum_2_device_not_in_phone_speaker",
        F.round(F.avg("max_rows_in_listener_id").over(w_source), 2).alias("source_avg_max_rows_in_listener_id"),
        F.min("total_ms_played_sum_2").over(w_source).alias("source_min_total_ms_played_sum_2"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        (F.col("avg_running_ms_played") - F.lag("avg_running_ms_played").over(w_all_by_max_rows_in_listener)).alias("avg_running_ms_played_change"),
        F.last("total_ms_played_sum_2", ignorenulls=True).over(w_all_by_max_rows_in_listener_2).alias("last_known_total_ms_played_sum_2"),
        F.row_number().over(w_all_by_max_rows_in_listener).alias("rn"),
    )
)

result = (
    sequenced_2
    .filter(F.col("rn").between(4, 9))
    .select(
        "source",
        "max_rows_in_listener_id",
        "total_ms_played_sum_2",
        "avg_running_ms_played",
        "listener_id_min_ms_played_to_second_ms_played_ra",
        "source_avg_max_rows_in_listener_id",
        "last_known_total_ms_played_sum_2",
        "rn",
        F.round(F.col("avg_running_ms_played") * 1.1, 2).alias("avg_running_ms_played_adj"),
        F.greatest(F.col("avg_running_ms_played"), F.col("total_ms_played_sum_2")).alias("greatest_avg_running_ms_played_total_ms_played_s"),
    )
    .orderBy(F.col("listener_id_min_ms_played_to_second_ms_played_ra").desc())
)
