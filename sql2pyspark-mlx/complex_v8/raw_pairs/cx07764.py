from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_running = Window.orderBy(F.col("event_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_event_at_year_event_ty = Window.partitionBy(F.year("first_event_at"), "event_type").orderBy(F.col("first_event_at").desc(), "id")
w_first_event_at_year_event_ty_2 = Window.partitionBy(F.year("first_event_at"), "event_type").orderBy(F.col("first_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_event_at_dense_rank = Window.partitionBy("first_event_at_dense_rank")
w_event_type_first_known_event = Window.partitionBy("event_type", F.year("first_known_event_at")).orderBy("first_known_event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_first_known_event_2 = Window.partitionBy("event_type", F.year("first_known_event_at")).orderBy("first_known_event_at")
w_first_known_event_at_dense_r = Window.partitionBy("first_known_event_at_dense_rank", F.year("last_last_first_event_at")).orderBy(F.col("first_known_event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_event_type_in_first = Window.partitionBy("distinct_event_type_in_first_event_at_dense_rank")

filtered_events = df_ticket_events.filter(F.col("event_type") == "created")

with_windows = (
    filtered_events
    .select(
        "id",
        "event_type",
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.first("event_at").over(w_actor_by_event_at_full).alias("first_event_at"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.lead("first_event_at").over(w_first_event_at_year_event_ty).alias("next_first_event_at"),
        F.last("first_event_at").over(w_first_event_at_year_event_ty_2).alias("last_first_event_at"),
        F.dense_rank().over(w_first_event_at_year_event_ty).alias("first_event_at_dense_rank"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.size(F.collect_set("event_type").over(w_first_event_at_dense_rank)).alias("distinct_event_type_in_first_event_at_dense_rank"),
        F.last("last_first_event_at").over(w_event_type_first_known_event).alias("last_last_first_event_at"),
        F.dense_rank().over(w_event_type_first_known_event_2).alias("first_known_event_at_dense_rank"),
        F.count(F.lit(1)).over(w_first_event_at_dense_rank).alias("rows_in_first_event_at_dense_rank"),
        F.min("first_event_at_dense_rank").over(w_first_event_at_dense_rank).alias("first_event_at_dense_rank_min_first_event_at_den"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.max("first_event_at_dense_rank").over(w_first_known_event_at_dense_r).alias("max_first_event_at_dense_rank_so_far"),
        F.size(F.collect_set("event_type").over(w_distinct_event_type_in_first)).alias("distinct_event_type_in_distinct_event_type_in_fi"),
    )
)

summary = (
    windowed_2
    .filter(F.col("distinct_event_type_in_distinct_event_type_in_fi") < 8)
    .groupBy(
        F.to_date("first_known_event_at").alias("first_known_event_at_day"),
        F.to_date("next_first_event_at").alias("next_first_event_at_day"),
        "event_type",
    )
    .agg(
        F.sum(F.when(F.col("event_type").isin("resolved", "escalated", "replied"), 1).otherwise(0)).alias("n_event_type_in_resolved_escal"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("n_rows"),
        F.round(F.avg("first_known_event_at_dense_rank"), 2).alias("avg_first_known_event_at_dense_rank"),
        F.max("last_first_event_at").alias("last_last_first_event_at_2"),
        F.sum("max_first_event_at_dense_rank_so_far").alias("sum_max_first_event_at_dense_rank_so_far_check"),
    )
    .filter(F.col("sum_max_first_event_at_dense_rank_so_far_check") > 31)
    .drop("sum_max_first_event_at_dense_rank_so_far_check")
)

without_matches = (
    summary
    .crossJoin(summary.agg(F.avg("n_event_type_in_resolved_escal").alias("overall_avg_n_event_type_in_resolved_escal")))
    .filter(F.col("n_event_type_in_resolved_escal") > F.col("overall_avg_n_event_type_in_resolved_escal"))
    .drop("overall_avg_n_event_type_in_resolved_escal")
)

result = (
    without_matches
    .select(
        "first_known_event_at_day",
        "next_first_event_at_day",
        "event_type",
        "n_event_type_in_resolved_escal",
        "event_type_list",
        "avg_first_known_event_at_dense_rank",
        "last_last_first_event_at_2",
    )
)
