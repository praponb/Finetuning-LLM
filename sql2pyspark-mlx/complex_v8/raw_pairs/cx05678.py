from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_priority_actor_by_tic = Window.partitionBy("ticket_priority", "actor").orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id")
w_event_type_actor_by_ticket_c = Window.partitionBy("event_type", "actor").orderBy(F.col("ticket_closed_at").desc(), "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_priority_actor_by_tic_2 = Window.partitionBy("ticket_priority", "actor").orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_dense_rank_2_event = Window.partitionBy("event_at_dense_rank_2", "event_type").orderBy("event_at_cume_dist", "id")
w_ticket_closed_at_seq_event_a = Window.partitionBy("ticket_closed_at_seq", "event_at_dense_rank")
w_all_by_seq_no_ties = Window.orderBy(F.col("seq_no").desc_nulls_first())
w_ticket_closed_at_dense_rank = Window.partitionBy("ticket_closed_at_dense_rank").orderBy("ticket_closed_at_decile_change_actor_agent", "ticket_closed_at_dense_rank").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_closed_at_dense_rank_2 = Window.partitionBy("ticket_closed_at_dense_rank").orderBy("ticket_closed_at_decile_change_actor_agent", "ticket_closed_at_dense_rank").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_ticket_events
    .filter((F.col("actor").isNotNull()) & (F.col("event_type") == "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

combined_details = (
    scoped.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank_2"),
        F.dense_rank().over(w_ticket_priority_actor_by_tic).alias("ticket_closed_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(10).over(w_event_type_actor_by_ticket_c).alias("ticket_closed_at_decile"),
        F.row_number().over(w_ticket_priority_actor_by_tic).alias("ticket_closed_at_seq"),
        F.lag("ticket_closed_at", 2).over(w_ticket_id_event_type_by_even).alias("prev2_ticket_closed_at"),
        F.round((F.unix_timestamp("ticket_closed_at") - F.unix_timestamp(F.lag("ticket_closed_at").over(w_ticket_id_event_type_by_even))) / 60, 1).alias("minutes_since_prev_ticket_closed_at"),
        F.last("ticket_closed_at").over(w_ticket_priority_actor_by_tic_2).alias("last_ticket_closed_at"),
        F.row_number().over(w_all_by_event_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_cume_dist_2"),
        F.round(F.percent_rank().over(w_all_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.lag("event_at", 2).over(w_ticket_id_event_type_by_even).alias("prev2_event_at"),
    )
)

top_rows = with_windows.filter(F.col("ticket_closed_at_dense_rank") <= 3)

metrics = (
    top_rows
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_agent_id",
        "event_at_dense_rank",
        "ticket_closed_at_dense_rank",
        "event_at_cume_dist",
        "ticket_closed_at_decile",
        "ticket_closed_at_seq",
        "prev2_ticket_closed_at",
        "minutes_since_prev_ticket_closed_at",
        "seq_no",
        "event_at_cume_dist_2",
        "event_at_percent_rank",
        "second_event_at",
        (F.col("ticket_closed_at_decile") - F.lag("ticket_closed_at_decile").over(w_event_at_dense_rank_2_event)).alias("ticket_closed_at_decile_change"),
        F.count(F.lit(1)).over(w_ticket_closed_at_seq_event_a).alias("rows_in_ticket_closed_at_seq_event_at_dense_rank"),
        F.size(F.collect_set("event_type").over(w_ticket_closed_at_seq_event_a)).alias("distinct_event_type_in_ticket_closed_at_seq_even"),
        F.round(F.avg("event_at_dense_rank").over(w_ticket_closed_at_seq_event_a), 2).alias("ticket_closed_at_seq_event_at_dense_rank_avg_eve"),
        F.round(F.percent_rank().over(w_all_by_seq_no_ties), 3).alias("seq_no_percent_rank"),
    )
)

summary = (
    metrics
    .filter(F.col("seq_no") < 8)
    .groupBy("ticket_closed_at_dense_rank")
    .agg(
        F.sum(F.when(F.col("actor") != "agent", F.col("ticket_closed_at_decile_change")).otherwise(0)).alias("ticket_closed_at_decile_change_actor_agent"),
        F.round(F.stddev("ticket_closed_at_seq_event_at_dense_rank_avg_eve"), 2).alias("stddev_ticket_closed_at_seq_event_at_dense_rank_"),
    )
)

windowed = (
    summary
    .select(
        "ticket_closed_at_dense_rank",
        "ticket_closed_at_decile_change_actor_agent",
        F.last("ticket_closed_at_dense_rank", ignorenulls=True).over(w_ticket_closed_at_dense_rank).alias("last_known_ticket_closed_at_dense_rank"),
        F.nth_value("ticket_closed_at_dense_rank", 2).over(w_ticket_closed_at_dense_rank_2).alias("second_ticket_closed_at_dense_rank"),
    )
)

result = (
    windowed
    .select(
        "ticket_closed_at_dense_rank",
        "ticket_closed_at_decile_change_actor_agent",
        "last_known_ticket_closed_at_dense_rank",
        "second_ticket_closed_at_dense_rank",
    )
    .orderBy(
        F.col("ticket_closed_at_decile_change_actor_agent").asc_nulls_last(),
        F.col("second_ticket_closed_at_dense_rank").desc(),
        "ticket_closed_at_dense_rank",
    )
    .limit(10)
)
