from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped = Window.partitionBy("is_skipped")
w_played_at_month_by_played_at = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_by_played_at_2 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("played_at")
w_is_skipped_label_track_id_by = Window.partitionBy("is_skipped_label", "track_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_month_by_played_at_3 = Window.partitionBy(F.date_trunc("month", "played_at")).orderBy("played_at", "id")
w_is_skipped_label_listener_id = Window.partitionBy("is_skipped_label", "listener_id").orderBy("ms_played", "id")
w_played_at_month_by_rows_in_i = Window.partitionBy(F.month("played_at")).orderBy(F.col("rows_in_is_skipped").desc(), "id")
w_played_at_rank = Window.partitionBy("played_at_rank")
w_played_at_rank_by_distinct_i = Window.partitionBy("played_at_rank").orderBy("distinct_is_skipped_label_in_is_skipped", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_is_skipped_avg_ms_pla = Window.orderBy("is_skipped_avg_ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_plays
    .filter((F.col("track_id").isNotNull()) & (F.col("played_at") >= F.lit("2025-01-19 06:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.date_format(F.col("played_at"), "MMM yyyy").alias("played_at_label"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.count(F.lit(1)).over(w_is_skipped).alias("rows_in_is_skipped"),
        F.first("played_at", ignorenulls=True).over(w_played_at_month_by_played_at).alias("first_known_played_at"),
        F.rank().over(w_played_at_month_by_played_at_2).alias("played_at_rank"),
        F.max("ms_played").over(w_is_skipped).alias("is_skipped_max_ms_played"),
        F.last("played_at").over(w_is_skipped_label_track_id_by).alias("last_played_at"),
        F.size(F.collect_set("is_skipped_label").over(w_is_skipped)).alias("distinct_is_skipped_label_in_is_skipped"),
        F.round(F.avg("ms_played").over(w_is_skipped), 2).alias("is_skipped_avg_ms_played"),
        F.lead("played_at").over(w_played_at_month_by_played_at_3).alias("next_played_at"),
        (F.col("ms_played") - F.lag("ms_played").over(w_is_skipped_label_listener_id)).alias("ms_played_change"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "is_skipped_label",
        "played_at_label",
        "rows_in_is_skipped",
        "first_known_played_at",
        "played_at_rank",
        "last_played_at",
        "distinct_is_skipped_label_in_is_skipped",
        "is_skipped_avg_ms_played",
        "next_played_at",
        F.lead("played_at_rank", 3).over(w_played_at_month_by_rows_in_i).alias("next3_played_at_rank"),
        F.round(F.avg("rows_in_is_skipped").over(w_played_at_rank), 2).alias("played_at_rank_avg_rows_in_is_skipped"),
        F.last("next_played_at", ignorenulls=True).over(w_played_at_rank_by_distinct_i).alias("last_known_next_played_at"),
        F.last("ms_played_change").over(w_all_by_is_skipped_avg_ms_pla).alias("last_ms_played_change"),
        F.round(F.col("is_skipped_max_ms_played") - F.avg("is_skipped_max_ms_played").over(w_played_at_rank), 2).alias("is_skipped_max_ms_played_vs_played_at_rank_avg"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_rank), 2).alias("ms_played_vs_played_at_rank_avg"),
        F.sum("played_at_rank").over(w_played_at_rank).alias("played_at_rank_sum_played_at_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "is_skipped_label",
        "played_at_label",
        "rows_in_is_skipped",
        "first_known_played_at",
        "played_at_rank",
        "last_played_at",
        "distinct_is_skipped_label_in_is_skipped",
        "is_skipped_avg_ms_played",
        "played_at_rank_avg_rows_in_is_skipped",
        "last_known_next_played_at",
        "is_skipped_max_ms_played_vs_played_at_rank_avg",
        "ms_played_vs_played_at_rank_avg",
        "played_at_rank_sum_played_at_rank",
        F.concat(F.col("played_at_label"), F.lit("#"), F.col("rows_in_is_skipped").cast("string")).alias("played_at_label_ref"),
    )
    .orderBy(F.col("last_played_at").desc(), "id")
    .limit(5)
)
