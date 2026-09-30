from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_by_is_skipped_fla = Window.partitionBy("is_skipped").orderBy("is_skipped_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_is_skipped_fla_2 = Window.partitionBy("is_skipped").orderBy("is_skipped_flag", "id")
w_device = Window.partitionBy("device")
w_all_by_ms_played_filled_shar = Window.orderBy("ms_played_filled_share_of_listener_id", "id")
w_device_by_second_is_skipped = Window.partitionBy("device").orderBy("second_is_skipped_flag", "id")
w_source_by_ms_played_ties = Window.partitionBy("source").orderBy("ms_played")
w_is_skipped_flag_second_is_sk = Window.partitionBy("is_skipped_flag", "second_is_skipped_flag_rank")
w_second_is_skipped_flag_by_is = Window.partitionBy("second_is_skipped_flag").orderBy(F.col("is_skipped_flag_second_is_skipped_flag_rank_sum_").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_skipped_label = Window.partitionBy("is_skipped_label")

base_plays = (
    df_plays
    .filter((F.col("played_at") >= F.lit("2025-01-13 12:00:00").cast("timestamp")) & (F.col("track_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.coalesce(F.col("device"), F.lit("UNKNOWN")).alias("device_or_unknown"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

metrics = (
    base_plays
    .select(
        "*",
        F.round(F.avg("ms_played_filled").over(w_listener_id), 2).alias("listener_id_avg_ms_played_filled"),
        F.first("is_skipped_flag").over(w_is_skipped_by_is_skipped_fla).alias("first_is_skipped_flag"),
        F.nth_value("is_skipped_flag", 2).over(w_is_skipped_by_is_skipped_fla).alias("second_is_skipped_flag"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_listener_id), F.lit(0)), 4).alias("ms_played_filled_share_of_listener_id"),
        F.round(F.percent_rank().over(w_is_skipped_by_is_skipped_fla_2), 3).alias("is_skipped_flag_percent_rank"),
    )
)

sequenced = (
    metrics
    .select(
        "*",
        F.round(F.col("first_is_skipped_flag") / F.nullif(F.sum("first_is_skipped_flag").over(w_device), F.lit(0)), 4).alias("first_is_skipped_flag_share_of_device"),
        F.max("is_skipped_flag_percent_rank").over(w_device).alias("device_max_is_skipped_flag_percent_rank"),
        F.lead("is_skipped_flag_percent_rank", 1, 0).over(w_all_by_ms_played_filled_shar).alias("next_is_skipped_flag_percent_rank"),
        F.min("is_skipped_flag_percent_rank").over(w_device).alias("device_min_is_skipped_flag_percent_rank"),
        F.sum("first_is_skipped_flag").over(w_device).alias("device_sum_first_is_skipped_flag"),
        F.rank().over(w_device_by_second_is_skipped).alias("second_is_skipped_flag_rank"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "listener_id",
        "ms_played",
        "is_skipped",
        "ms_played_filled",
        "device_or_unknown",
        "is_skipped_flag",
        "first_is_skipped_flag",
        "second_is_skipped_flag",
        "device_max_is_skipped_flag_percent_rank",
        "next_is_skipped_flag_percent_rank",
        "device_sum_first_is_skipped_flag",
        "second_is_skipped_flag_rank",
        F.rank().over(w_source_by_ms_played_ties).alias("ms_played_rank"),
        F.sum("ms_played").over(w_is_skipped_flag_second_is_sk).alias("is_skipped_flag_second_is_skipped_flag_rank_sum_"),
        F.size(F.collect_set("listener_id").over(w_is_skipped_flag_second_is_sk)).alias("distinct_listener_id_in_is_skipped_flag_second_i"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped_flag_second_is_sk), 2).alias("ms_played_vs_is_skipped_flag_second_is_skipped_f"),
    )
)

tagged = (
    sequenced_2
    .filter(F.col("next_is_skipped_flag_percent_rank").between(0.1, 0.8))
    .withColumn("is_skipped_label", F.when(F.col("is_skipped"), "yes").otherwise("no"))
)

is_skipped_label_stats = (
    tagged
    .groupBy("is_skipped_label")
    .agg(F.max("first_is_skipped_flag").alias("max_first_is_skipped_flag"))
)

outliers = (
    tagged.alias("s")
    .join(is_skipped_label_stats.alias("g"), F.col("g.is_skipped_label") == F.col("s.is_skipped_label"), "inner")
    .filter(F.col("s.first_is_skipped_flag") == F.col("g.max_first_is_skipped_flag"))
    .select(
        "s.*",
        "g.max_first_is_skipped_flag",
        F.round(F.col("s.first_is_skipped_flag") - F.col("g.max_first_is_skipped_flag"), 2).alias("first_is_skipped_flag_minus_max"),
    )
)

windowed = (
    outliers
    .withColumn("ms_played_max_2", F.max("ms_played").over(w_second_is_skipped_flag_by_is))
    .withColumn("max_first_is_skipped_flag_share_of_is_skipped_la", F.round(F.col("max_first_is_skipped_flag") / F.nullif(F.sum("max_first_is_skipped_flag").over(w_is_skipped_label), F.lit(0)), 4))
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "is_skipped",
        "ms_played_filled",
        "device_or_unknown",
        "is_skipped_flag",
        "second_is_skipped_flag",
        "device_max_is_skipped_flag_percent_rank",
        "next_is_skipped_flag_percent_rank",
        "device_sum_first_is_skipped_flag",
        "second_is_skipped_flag_rank",
        "distinct_listener_id_in_is_skipped_flag_second_i",
        "is_skipped_label",
        "max_first_is_skipped_flag",
        "first_is_skipped_flag_minus_max",
        "ms_played_max_2",
        "max_first_is_skipped_flag_share_of_is_skipped_la",
        (F.col("ms_played") + F.col("ms_played_max_2")).alias("ms_played_plus_ms_played_max_2"),
    )
    .orderBy(F.col("id").desc(), F.col("first_is_skipped_flag_minus_max").desc())
    .limit(10)
)
