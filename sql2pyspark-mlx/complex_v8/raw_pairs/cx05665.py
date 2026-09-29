from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_played_at_year_by_ms_played = Window.partitionBy(F.year("played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_year_by_ms_played_2 = Window.partitionBy(F.year("played_at")).orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_year_by_ms_played_3 = Window.partitionBy(F.year("played_at")).orderBy("ms_played")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_source_device = Window.partitionBy("source", "device")
w_source_played_at_year_by_ms = Window.partitionBy("source", F.year("played_at")).orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_is_skipped_by_sourc = Window.partitionBy("track_id", "is_skipped").orderBy("source_len", "id")
w_device_source_len_by_source = Window.partitionBy("device", "source_len").orderBy("source_len", "id")

valid = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.size(F.collect_set("source").over(w_track_id)).alias("distinct_source_in_track_id"),
        F.nth_value("ms_played", 2).over(w_played_at_year_by_ms_played).alias("second_ms_played"),
        F.last("source_len", ignorenulls=True).over(w_played_at_year_by_ms_played_2).alias("last_known_source_len"),
        F.round(F.cume_dist().over(w_played_at_year_by_ms_played_3), 3).alias("ms_played_cume_dist"),
        (F.col("ms_played") - F.lag("ms_played").over(w_all_by_played_at)).alias("ms_played_change"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_source_device), F.lit(0)), 4).alias("ms_played_share_of_source_device"),
        F.last("played_at", ignorenulls=True).over(w_source_played_at_year_by_ms).alias("last_known_played_at"),
        F.count(F.lit(1)).over(w_source_device).alias("rows_in_source_device"),
        F.lead("played_at").over(w_track_id_is_skipped_by_sourc).alias("next_played_at"),
        F.rank().over(w_device_source_len_by_source).alias("source_len_rank"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.coalesce(F.col("second_ms_played"), F.lit(0)).alias("second_ms_played_filled"),
        F.abs(F.col("source_len_rank") - 6).alias("source_len_rank_distance"),
    )
)

summary = (
    labeled
    .groupBy("distinct_source_in_track_id")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.max("played_at").alias("last_played_at"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("n_is_skipped_false"),
        F.max("ms_played").alias("max_ms_played_check"),
    )
    .filter(F.col("max_ms_played_check") > 99376)
    .drop("max_ms_played_check")
)

result = (
    summary
    .select("distinct_source_in_track_id", "last_played_at", "n_is_skipped_false")
    .orderBy("n_is_skipped_false")
)
