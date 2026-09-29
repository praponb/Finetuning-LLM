from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_source = Window.partitionBy("source")
w_all_by_ms_played_moving_6 = Window.orderBy("ms_played", "id").rowsBetween(-6, Window.currentRow)
w_all_by_ms_played_prior_2 = Window.orderBy("ms_played", "id").rowsBetween(-2, -1)
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_device_by_last_played_at_plu = Window.partitionBy("device").orderBy("last_played_at_plus_30m", "device", "is_skipped")
w_device_by_last_played_at_plu_2 = Window.partitionBy("device").orderBy("last_played_at_plus_30m", "device", "is_skipped").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_plays = (
    df_plays
    .filter((F.col("ms_played") > 145720) & (F.col("source") == "playlist"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.col("played_at") + F.expr("INTERVAL 30 MINUTES")).alias("played_at_plus_30m"),
        F.concat(F.col("device"), F.lit("#"), F.col("listener_id").cast("string")).alias("device_ref"),
    )
)

sequenced = (
    scoped_plays
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "played_at_plus_30m",
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_all_by_ms_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.size(F.collect_set("device").over(w_source)).alias("distinct_device_in_source"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_6), 2).alias("ms_played_avg_7"),
        F.round(F.avg("ms_played").over(w_source), 2).alias("source_avg_ms_played"),
        F.round(F.percent_rank().over(w_all_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.sum("ms_played").over(w_all_by_ms_played_prior_2).alias("ms_played_sum_2_prior"),
        F.sum("ms_played").over(w_all_by_ms_played_running).alias("running_ms_played"),
        F.sum("ms_played").over(w_source).alias("source_sum_ms_played"),
    )
)

enriched = (
    sequenced
    .select(
        "id",
        "track_id",
        "device",
        "is_skipped",
        "played_at_plus_30m",
        "source_avg_ms_played",
        "ms_played_percent_rank",
        "ms_played_sum_2_prior",
        "running_ms_played",
        F.round(F.col("ms_played_percent_rank") * 1.1, 2).alias("ms_played_percent_rank_adj"),
    )
)

totals = (
    enriched
    .groupBy("device", "is_skipped")
    .agg(
        F.round(100 * F.count(F.when(~F.col("is_skipped"), 1)) / F.count("*"), 1).alias("pct_not_is_skipped"),
        F.count(F.when(F.col("device") != "car", 1)).alias("n_device_car"),
        F.round(F.sum("source_avg_ms_played") / F.nullif(F.sum("ms_played_percent_rank_adj"), F.lit(0)), 3).alias("source_avg_ms_played_to_ms_played_percent_rank_a"),
        F.round(F.sum("running_ms_played") / F.nullif(F.sum("ms_played_percent_rank"), F.lit(0)), 3).alias("running_ms_played_to_ms_played_percent_rank_rati"),
        F.max("played_at_plus_30m").alias("last_played_at_plus_30m"),
    )
)

metrics = (
    totals
    .select(
        "device",
        "is_skipped",
        "pct_not_is_skipped",
        "source_avg_ms_played_to_ms_played_percent_rank_a",
        "running_ms_played_to_ms_played_percent_rank_rati",
        F.round(F.percent_rank().over(w_device_by_last_played_at_plu), 3).alias("last_played_at_plus_30m_percent_rank"),
        F.last("last_played_at_plus_30m", ignorenulls=True).over(w_device_by_last_played_at_plu_2).alias("last_known_last_played_at_plus_30m"),
    )
)

result = (
    metrics
    .select(
        "device",
        "is_skipped",
        "pct_not_is_skipped",
        "source_avg_ms_played_to_ms_played_percent_rank_a",
        "running_ms_played_to_ms_played_percent_rank_rati",
        "last_played_at_plus_30m_percent_rank",
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
    .orderBy(F.col("is_skipped_label").desc_nulls_first())
)
