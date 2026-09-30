from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_or_unknown_by_ms_play = Window.partitionBy("device_or_unknown").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-1, -1)
w_listener_id_device = Window.partitionBy("listener_id", "device")
w_device_or_unknown_by_ms_play_2 = Window.partitionBy("device_or_unknown").orderBy(F.col("ms_played").desc(), "id")
w_is_skipped_device_or_unknown = Window.partitionBy("is_skipped", "device_or_unknown").orderBy(F.col("ms_played").desc(), "id")
w_device_is_skipped_by_ms_play = Window.partitionBy("device", "is_skipped").orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_device_or_unknown_by_ms_play_3 = Window.partitionBy("device_or_unknown").orderBy(F.col("ms_played").desc(), "id").rowsBetween(-6, Window.currentRow)
w_source_is_skipped_by_ms_play = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_is_skipped_by_ms_play_2 = Window.partitionBy("device", "is_skipped").orderBy(F.col("ms_played").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped = Window.partitionBy("is_skipped")
w_source_is_skipped_by_ms_play_2 = Window.partitionBy("source", "is_skipped").orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_plays
    .filter((F.col("source").isin("search", "radio", "playlist")) & (F.col("device") != "car"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "device_or_unknown",
        "source_slug",
        F.round(F.avg("ms_played").over(w_device_or_unknown_by_ms_play), 2).alias("ms_played_avg_1_prior"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id_device), 2).alias("ms_played_vs_listener_id_device_avg"),
        F.rank().over(w_device_or_unknown_by_ms_play_2).alias("ms_played_rank"),
        F.row_number().over(w_is_skipped_device_or_unknown).alias("seq_no"),
        F.ntile(4).over(w_is_skipped_device_or_unknown).alias("ms_played_quartile"),
        F.row_number().over(w_device_is_skipped_by_ms_play).alias("rn"),
        F.round(F.avg("ms_played").over(w_device_or_unknown_by_ms_play_3), 2).alias("ms_played_avg_7"),
        F.ntile(5).over(w_device_is_skipped_by_ms_play).alias("ms_played_quintile"),
        F.first("ms_played", ignorenulls=True).over(w_source_is_skipped_by_ms_play).alias("first_known_ms_played"),
        F.first("ms_played").over(w_device_is_skipped_by_ms_play_2).alias("first_ms_played"),
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.nth_value("ms_played", 2).over(w_source_is_skipped_by_ms_play_2).alias("second_ms_played"),
    )
)

summary = (
    sequenced
    .groupBy("listener_id")
    .agg(
        F.array_sort(F.collect_set("device")).alias("device_list"),
        F.countDistinct("device").alias("distinct_devices"),
        F.count("*").alias("cnt"),
        F.round(F.sum("ms_played_avg_7"), 2).alias("total_ms_played_avg_7"),
        F.max("ms_played_avg_1_prior").alias("max_ms_played_avg_1_prior_check"),
    )
    .filter(F.col("max_ms_played_avg_1_prior_check") > 84000.0)
    .drop("max_ms_played_avg_1_prior_check")
)

result = (
    summary
    .select(
        "listener_id",
        "device_list",
        "distinct_devices",
        "cnt",
    )
)
