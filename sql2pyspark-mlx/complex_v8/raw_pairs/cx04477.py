from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_max_ms_played = Window.orderBy(F.col("max_ms_played").asc_nulls_last(), "is_skipped")
w_max_is_skipped_flag_min_is_s = Window.partitionBy("max_is_skipped_flag", "min_is_skipped_flag")
w_all_by_max_is_skipped_flag = Window.orderBy("max_is_skipped_flag", "is_skipped")
w_max_is_skipped_flag_is_skipp = Window.partitionBy("max_is_skipped_flag", "is_skipped").orderBy("min_is_skipped_flag", "is_skipped")

filtered_plays = (
    df_plays
    .filter((F.col("listener_id") >= 3) & (F.to_date(F.col("played_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "is_skipped",
        "source",
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
    )
)

stats = (
    filtered_plays
    .groupBy("is_skipped")
    .agg(
        F.max("ms_played").alias("max_ms_played"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.min("is_skipped_flag").alias("min_is_skipped_flag"),
        F.sum(F.when(F.col("is_skipped"), 1).otherwise(0)).alias("n_is_skipped_true"),
        F.max("is_skipped_flag").alias("max_is_skipped_flag"),
    )
)

metrics = (
    stats
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_max_ms_played), 3).alias("max_ms_played_percent_rank"),
        F.round(F.col("max_is_skipped_flag") - F.avg("max_is_skipped_flag").over(w_max_is_skipped_flag_min_is_s), 2).alias("max_is_skipped_flag_vs_max_is_skipped_flag_min_i"),
    )
)

with_windows = (
    metrics
    .select(
        "is_skipped",
        "max_ms_played",
        "source_list",
        "min_is_skipped_flag",
        "n_is_skipped_true",
        "max_ms_played_percent_rank",
        F.lead("max_ms_played", 3, 0).over(w_all_by_max_is_skipped_flag).alias("next3_max_ms_played"),
        F.ntile(5).over(w_all_by_max_is_skipped_flag).alias("max_is_skipped_flag_quintile"),
        F.lag("n_is_skipped_true", 1, 0).over(w_max_is_skipped_flag_is_skipp).alias("prev_n_is_skipped_true"),
    )
)

result = (
    with_windows
    .select(
        "is_skipped",
        F.col("n_is_skipped_true").alias("n_is_skipped_true_v"),
        F.col("prev_n_is_skipped_true").alias("m_prev_n_is_skipped_true"),
    )
    .unpivot(
        ids=["is_skipped"],
        values=["n_is_skipped_true_v", "m_prev_n_is_skipped_true"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
