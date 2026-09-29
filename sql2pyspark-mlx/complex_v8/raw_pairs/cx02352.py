from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_priority_ticket_id_by = Window.partitionBy("ticket_priority", "ticket_id").orderBy("event_at_month", "id")
w_ticket_priority_by_event_at = Window.partitionBy("ticket_priority").orderBy("event_at", "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_month = Window.orderBy("event_at_month", "id")
w_all_by_event_at_month_full = Window.orderBy("event_at_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.month(F.col("event_at")).isin(2, 3)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.concat_ws(": ", F.col("event_type"), F.col("actor")).alias("event_type_actor_label"),
    )
)

with_parent = (
    base_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat").between(2, 5)), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("su.tier").alias("agent_tier"),
    )
)

metrics = (
    with_parent
    .select(
        "*",
        F.round(F.percent_rank().over(w_ticket_priority_ticket_id_by), 3).alias("event_at_month_percent_rank"),
        F.round(F.cume_dist().over(w_ticket_priority_by_event_at), 3).alias("event_at_cume_dist"),
        F.round((F.unix_timestamp("event_at_month") - F.unix_timestamp(F.lag("event_at_month").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at_month"),
        F.row_number().over(w_ticket_id_by_event_at).alias("rn"),
        F.lag("event_at_month").over(w_all_by_event_at_month).alias("prev_event_at_month"),
        F.nth_value("event_at_month", 2).over(w_all_by_event_at_month_full).alias("second_event_at_month"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.row_number().over(w_all_by_event_at_month).alias("event_at_month_seq"),
    )
)

leaders = (
    metrics
    .filter(F.col("rn") <= 3)
    .drop("rn")
)

result = (
    leaders
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_month",
        "ticket_tags",
        "agent_tier",
        "event_at_month_percent_rank",
        "event_at_cume_dist",
        "minutes_since_prev_event_at_month",
        "prev_event_at_month",
        "event_at_month_seq",
        F.concat_ws(": ", F.col("agent_tier"), F.col("actor")).alias("agent_tier_actor_label"),
        F.when(F.col("event_type").isin("replied", "reopened"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
    .orderBy(F.col("event_at_month").desc(), F.col("event_at").desc())
)
