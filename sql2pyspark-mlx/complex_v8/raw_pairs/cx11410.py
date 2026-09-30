from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_device_in_speaker_p = Window.orderBy("n_device_in_speaker_phone", "listener_id", "source").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_device_in_speaker_p_2 = Window.orderBy("n_device_in_speaker_phone", "listener_id", "source").rowsBetween(-3, Window.currentRow)
w_source_by_n_device_in_speake = Window.partitionBy("source").orderBy(F.col("n_device_in_speaker_phone_max_4").desc(), "listener_id", "source").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_n_device_in_speaker_p = Window.partitionBy("source", "n_device_in_speaker_phone")
w_source_by_n_device_in_speake_2 = Window.partitionBy("source").orderBy(F.col("n_device_in_speaker_phone_max_4").desc(), "listener_id", "source")

cleaned = (
    df_plays
    .filter((F.hour(F.col("played_at")).between(10, 15)) & (F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM-dd HH:00").alias("played_at_label"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

source_stats = (
    cleaned
    .groupBy("source")
    .agg(F.max("ms_played").alias("max_ms_played"))
)

above_average = (
    cleaned.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played") == F.col("g.max_ms_played"))
    .select(
        "s.*",
        "g.max_ms_played",
        F.round(F.col("s.ms_played") - F.col("g.max_ms_played"), 2).alias("ms_played_minus_max"),
    )
)

rollup_base = (
    above_average
    .filter(F.col("played_at_label").isNotNull())
    .groupBy("listener_id", "source")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.sum(F.when(F.col("device").isin("speaker", "phone"), 1).otherwise(0)).alias("n_device_in_speaker_phone"),
        F.sum("is_skipped_flag").alias("sum_is_skipped_flag_check"),
    )
    .filter(F.col("sum_is_skipped_flag_check") > 0)
    .drop("sum_is_skipped_flag_check")
)

windowed = (
    rollup_base
    .select(
        "listener_id",
        "source",
        "first_played_at",
        "n_device_in_speaker_phone",
        F.nth_value("first_played_at", 2).over(w_all_by_n_device_in_speaker_p).alias("second_first_played_at"),
        F.max("n_device_in_speaker_phone").over(w_all_by_n_device_in_speaker_p_2).alias("n_device_in_speaker_phone_max_4"),
    )
)

combined_details_listener = (
    windowed.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.listener_id",
        "s.source",
        "s.first_played_at",
        "s.n_device_in_speaker_phone",
        "s.second_first_played_at",
        "s.n_device_in_speaker_phone_max_4",
        F.col("l.interests").alias("listener_interests"),
    )
)

ranked = (
    combined_details_listener
    .select(
        "*",
        F.last("second_first_played_at").over(w_source_by_n_device_in_speake).alias("last_second_first_played_at"),
        F.size(F.collect_set("listener_id").over(w_source_n_device_in_speaker_p)).alias("distinct_listener_id_in_source_n_device_in_speak"),
        F.count(F.lit(1)).over(w_source_n_device_in_speaker_p).alias("rows_in_source_n_device_in_speaker_phone"),
        F.round(F.col("n_device_in_speaker_phone") - F.avg("n_device_in_speaker_phone").over(w_source_n_device_in_speaker_p), 2).alias("n_device_in_speaker_phone_vs_source_n_device_in_"),
        F.row_number().over(w_source_by_n_device_in_speake_2).alias("rn"),
    )
)

result = (
    ranked
    .select(
        "listener_id",
        "source",
        "second_first_played_at",
        "n_device_in_speaker_phone_max_4",
        "last_second_first_played_at",
        "rows_in_source_n_device_in_speaker_phone",
        "n_device_in_speaker_phone_vs_source_n_device_in_",
        "rn",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
    .orderBy(F.col("second_first_played_at").desc_nulls_first())
)
