from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ticket_requester_id = Window.orderBy(F.col("ticket_requester_id").desc(), "id")
w_ticket_priority = Window.partitionBy("ticket_priority")
w_all_by_event_at_prior_6 = Window.orderBy("event_at", "id").rowsBetween(-6, -1)
w_all_by_event_at = Window.orderBy("event_at", "id")
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_id_by_ticket_requeste = Window.partitionBy("ticket_id").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_actor = Window.partitionBy("ticket_id", "actor")
w_event_at_month_ticket_priori = Window.partitionBy(F.month("event_at"), "ticket_priority").orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id")
w_ticket_id_by_ticket_requeste_2 = Window.partitionBy("ticket_id").orderBy("ticket_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_ticket_requester_id_f = Window.orderBy(F.col("ticket_requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_month_ticket_priori_2 = Window.partitionBy(F.month("event_at"), "ticket_priority").orderBy(F.col("ticket_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(10, 14)) & (F.col("event_type") == "escalated"))
)

with_details_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

windowed = (
    with_details_ticket
    .select(
        "*",
        F.rank().over(w_all_by_ticket_requester_id).alias("ticket_requester_id_rank"),
        F.size(F.collect_set("event_type").over(w_ticket_priority)).alias("distinct_event_type_in_ticket_priority"),
        F.sum("ticket_requester_id").over(w_all_by_event_at_prior_6).alias("ticket_requester_id_sum_6_prior"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.max("ticket_requester_id").over(w_ticket_priority).alias("ticket_priority_max_ticket_requester_id"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_priority), 2).alias("ticket_requester_id_vs_ticket_priority_avg"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_id), 2).alias("ticket_requester_id_vs_ticket_id_avg"),
        F.last("ticket_closed_at", ignorenulls=True).over(w_ticket_id_by_ticket_requeste).alias("last_known_ticket_closed_at"),
        F.round(F.col("ticket_requester_id") - F.avg("ticket_requester_id").over(w_ticket_id_actor), 2).alias("ticket_requester_id_vs_ticket_id_actor_avg"),
        F.round(F.avg("ticket_requester_id").over(w_ticket_priority), 2).alias("ticket_priority_avg_ticket_requester_id"),
        F.rank().over(w_event_at_month_ticket_priori).alias("ticket_closed_at_rank"),
        F.row_number().over(w_all_by_event_at).alias("event_at_seq"),
        F.nth_value("ticket_closed_at", 2).over(w_ticket_id_by_ticket_requeste_2).alias("second_ticket_closed_at"),
        F.nth_value("event_at", 2).over(w_all_by_ticket_requester_id_f).alias("second_event_at"),
        F.first("event_at", ignorenulls=True).over(w_event_at_month_ticket_priori_2).alias("first_known_event_at"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("event_at_seq") <= 2) & (F.col("ticket_priority_avg_ticket_requester_id").between(7.0, 19.0)))
    .drop("event_at_seq")
)

result = (
    top_per_group
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "ticket_closed_at",
        "ticket_priority",
        "ticket_requester_id",
        "ticket_requester_id_sum_6_prior",
        "event_at_cume_dist",
        "ticket_priority_max_ticket_requester_id",
        "ticket_requester_id_vs_ticket_priority_avg",
        "ticket_requester_id_vs_ticket_id_avg",
        "last_known_ticket_closed_at",
        "ticket_priority_avg_ticket_requester_id",
        "ticket_closed_at_rank",
        "second_event_at",
        "first_known_event_at",
        F.upper(F.trim(F.col("ticket_priority"))).alias("ticket_priority_clean"),
    )
    .orderBy(F.col("last_known_ticket_closed_at").asc())
)
