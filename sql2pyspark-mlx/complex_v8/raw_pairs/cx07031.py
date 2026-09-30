from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_running = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at = Window.orderBy("event_at", "id")
w_event_type_by_event_at_ties = Window.partitionBy("event_type").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_running_2 = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_by_event_at = Window.partitionBy("event_type", "actor").orderBy("event_at_dense_rank_2", "id")
w_event_at_dense_rank_by_seq_n = Window.partitionBy("event_at_dense_rank").orderBy(F.col("seq_no").desc())
w_seq_no_by_event_at = Window.partitionBy("seq_no").orderBy("event_at", "id")
w_ticket_id_first_event_at_to = Window.partitionBy("ticket_id", F.to_date("first_event_at"))
w_event_type_actor_by_event_at_2 = Window.partitionBy("event_type", "actor").orderBy("event_at_dense_rank_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_actor_by_event_at_3 = Window.partitionBy("event_type", "actor").orderBy("event_at_dense_rank_2")
w_event_at_dense_rank_2_n_rows = Window.partitionBy("event_at_dense_rank_2", "n_rows").orderBy(F.col("avg_event_at_dense_rank_2").desc_nulls_first(), "event_at_dense_rank", "event_at_dense_rank_2").rowsBetween(-2, Window.currentRow)
w_n_rows_event_at_dense_rank_2 = Window.partitionBy("n_rows", "event_at_dense_rank_2").orderBy("avg_minutes_since_prev_event_at_2", "event_at_dense_rank", "event_at_dense_rank_2")

base_events = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(10, 12))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    base_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.first("event_at", ignorenulls=True).over(w_all_by_event_at_running).alias("first_known_event_at"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.first("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("first_known_event_at_2"),
        F.round(F.cume_dist().over(w_event_type_by_event_at), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_all_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.dense_rank().over(w_event_type_by_event_at_ties).alias("event_at_dense_rank_2"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.last("event_at", ignorenulls=True).over(w_all_by_event_at_running_2).alias("last_known_event_at"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_actor_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.rank().over(w_event_at_dense_rank_by_seq_n).alias("seq_no_rank"),
        F.ntile(4).over(w_seq_no_by_event_at).alias("event_at_quartile"),
        F.max("seq_no").over(w_ticket_id_first_event_at_to).alias("ticket_id_first_event_at_to_date_max_seq_no"),
        F.round(F.col("event_at_cume_dist") - F.avg("event_at_cume_dist").over(w_ticket_id_first_event_at_to), 2).alias("event_at_cume_dist_vs_ticket_id_first_event_at_t"),
        F.sum("event_at_dense_rank_2").over(w_event_type_actor_by_event_at_2).alias("running_event_at_dense_rank_2"),
        F.round(F.percent_rank().over(w_event_type_actor_by_event_at_3), 3).alias("event_at_dense_rank_2_percent_rank"),
        F.round(F.col("event_at_dense_rank") - F.avg("event_at_dense_rank").over(w_ticket_id_first_event_at_to), 2).alias("event_at_dense_rank_vs_ticket_id_first_event_at_"),
    )
)

totals = (
    ranked
    .groupBy("event_at_dense_rank", "event_at_dense_rank_2")
    .agg(
        F.round(F.avg("minutes_since_prev_event_at_2"), 2).alias("avg_minutes_since_prev_event_at_2"),
        F.round(F.avg("seq_no_rank"), 2).alias("avg_seq_no_rank"),
        F.sum(F.when(F.col("event_type").isin("escalated", "replied"), F.col("minutes_since_prev_event_at")).otherwise(0)).alias("minutes_since_prev_event_at_event_type_in_escala"),
        F.count("*").alias("n_rows"),
        F.max("event_at_cume_dist_vs_ticket_id_first_event_at_t").alias("max_event_at_cume_dist_vs_ticket_id_first_event_"),
        F.round(F.avg("event_at_dense_rank_2"), 2).alias("avg_event_at_dense_rank_2"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.round(F.avg("event_at_dense_rank").over(w_event_at_dense_rank_2_n_rows), 2).alias("event_at_dense_rank_avg_3"),
        F.lag("event_at_dense_rank_2", 1, 0).over(w_n_rows_event_at_dense_rank_2).alias("prev_event_at_dense_rank_2"),
    )
)

result = (
    with_windows
    .select(
        "event_at_dense_rank",
        "avg_minutes_since_prev_event_at_2",
        "avg_seq_no_rank",
        "minutes_since_prev_event_at_event_type_in_escala",
        "n_rows",
        "avg_event_at_dense_rank_2",
        "event_at_dense_rank_avg_3",
        "prev_event_at_dense_rank_2",
        F.coalesce(F.col("event_at_dense_rank_2"), F.lit(0)).alias("event_at_dense_rank_2_filled"),
        F.when(F.col("n_rows") >= 8, "HIGH").when(F.col("n_rows") >= 4, "MEDIUM").otherwise("LOW").alias("n_rows_tier"),
    )
    .orderBy(F.col("event_at_dense_rank_2_filled").asc_nulls_last())
)
