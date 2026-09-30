from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")
w_is_skipped_by_played_at_mont = Window.partitionBy("is_skipped").orderBy(F.col("played_at_month").asc_nulls_last(), "id")
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_distinct_listener_ids_by_dis = Window.partitionBy("distinct_listener_ids").orderBy("distinct_listener_ids", "is_skipped", "rn")
w_distinct_listener_ids_by_dis_2 = Window.partitionBy("distinct_listener_ids").orderBy("distinct_listener_ids", "is_skipped", "rn").rowsBetween(-1, Window.currentRow)
w_is_skipped = Window.partitionBy("is_skipped")
w_rn_by_distinct_listener_ids = Window.partitionBy("rn").orderBy("distinct_listener_ids_avg_2", "is_skipped", "rn").rowsBetween(-3, Window.currentRow)

recent = (
    df_plays
    .filter((F.col("played_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")) & (~F.col("device").isin("tv", "car")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.when(F.col("device").isin("speaker", "phone"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.round(F.percent_rank().over(w_listener_id_by_played_at_tie), 3).alias("played_at_percent_rank"),
        F.row_number().over(w_is_skipped_by_played_at_mont).alias("rn"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_is_skipped_by_played_at_mont))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.lag("played_at").over(w_is_skipped_by_played_at_mont).alias("prev_played_at"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at), 3).alias("played_at_percent_rank_2"),
        F.lead("played_at_month").over(w_listener_id_by_played_at).alias("next_played_at_month"),
    )
)

stats = (
    metrics
    .filter(F.col("listener_id") <= 2)
    .groupBy("is_skipped", "rn")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.avg("minutes_since_prev_played_at").alias("avg_minutes_since_prev_played_at_check"),
    )
    .filter(F.col("avg_minutes_since_prev_played_at_check") > 1500.0)
    .drop("avg_minutes_since_prev_played_at_check")
)

sequenced = (
    stats
    .select(
        "is_skipped",
        "rn",
        "row_count",
        "distinct_listener_ids",
        F.lag("distinct_listener_ids", 1, 0).over(w_distinct_listener_ids_by_dis).alias("prev_distinct_listener_ids"),
        F.round(F.avg("distinct_listener_ids").over(w_distinct_listener_ids_by_dis_2), 2).alias("distinct_listener_ids_avg_2"),
    )
)

without_matches = (
    sequenced
    .crossJoin(sequenced.agg(F.avg("distinct_listener_ids").alias("overall_avg_distinct_listener_ids")))
    .filter(F.col("distinct_listener_ids") > F.col("overall_avg_distinct_listener_ids"))
    .drop("overall_avg_distinct_listener_ids")
)

metrics_2 = (
    without_matches
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.round(F.col("distinct_listener_ids") - F.avg("distinct_listener_ids").over(w_is_skipped), 2).alias("distinct_listener_ids_vs_is_skipped_avg"),
        F.round(F.avg("row_count").over(w_rn_by_distinct_listener_ids), 2).alias("row_count_avg_4"),
        F.sum("row_count").over(w_is_skipped).alias("is_skipped_sum_row_count"),
    )
)

result = (
    metrics_2
    .groupingSets(
        [["row_count", "is_skipped", "rn"], ["row_count"], ["is_skipped"]],
        "row_count", "is_skipped", "rn",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("is_skipped_sum_row_count") / F.nullif(F.sum("rn"), F.lit(0)), 3).alias("is_skipped_sum_row_count_to_rn_ratio"),
        F.sum("is_skipped_sum_row_count").alias("total_is_skipped_sum_row_count"),
    )
)
