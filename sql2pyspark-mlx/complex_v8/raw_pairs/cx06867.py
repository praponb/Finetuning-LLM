from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_flag_source_len_b = Window.partitionBy("is_skipped_flag", "source_len").orderBy(F.col("is_skipped_flag_minus_max").asc_nulls_last(), "id")
w_source_len = Window.partitionBy("source_len")
w_is_skipped_flag_source_by_is = Window.partitionBy("is_skipped_flag", "source").orderBy("is_skipped_flag", "id")

base_plays = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
    )
)

tagged = (
    base_plays
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "is_skipped_flag",
        "ms_played_bucket",
        "device_clean",
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

source_stats = (
    tagged
    .groupBy("source")
    .agg(F.max("is_skipped_flag").alias("max_is_skipped_flag"))
)

above_average = (
    tagged.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.is_skipped_flag") == F.col("g.max_is_skipped_flag"))
    .select(
        "s.*",
        "g.max_is_skipped_flag",
        F.round(F.col("s.is_skipped_flag") - F.col("g.max_is_skipped_flag"), 2).alias("is_skipped_flag_minus_max"),
    )
)

ranked = (
    above_average
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_skipped_flag_source_len_b), 3).alias("is_skipped_flag_minus_max_cume_dist"),
        F.dense_rank().over(w_is_skipped_flag_source_len_b).alias("is_skipped_flag_minus_max_dense_rank"),
        F.round(F.col("max_is_skipped_flag") / F.nullif(F.sum("max_is_skipped_flag").over(w_source_len), F.lit(0)), 4).alias("max_is_skipped_flag_share_of_source_len"),
        F.row_number().over(w_is_skipped_flag_source_by_is).alias("rn"),
        F.lag("is_skipped_flag").over(w_is_skipped_flag_source_len_b).alias("prev_is_skipped_flag"),
        F.round(F.col("source_len") / F.nullif(F.sum("source_len").over(w_source_len), F.lit(0)), 4).alias("source_len_share_of_source_len"),
    )
)

result = (
    ranked
    .select(
        "id",
        "listener_id",
        "device",
        "is_skipped",
        "source",
        "is_skipped_flag",
        "ms_played_bucket",
        "device_clean",
        "is_skipped_flag_minus_max",
        "is_skipped_flag_minus_max_cume_dist",
        "is_skipped_flag_minus_max_dense_rank",
        "max_is_skipped_flag_share_of_source_len",
        "rn",
        "prev_is_skipped_flag",
        "source_len_share_of_source_len",
    )
)
