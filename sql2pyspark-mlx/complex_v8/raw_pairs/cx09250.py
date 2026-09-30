from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_closed_at_month = Window.partitionBy(F.date_trunc("month", "closed_at"))
w_all_by_closed_at_moving_2 = Window.orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(7, 12)) & (F.col("priority") == "P1"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_activity = (
    filtered_tickets.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-08").cast("date")) & (F.col("t.event_type") == "escalated"), "left_anti")
)

channel_stats = (
    with_activity
    .groupBy("channel")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

vs_group = (
    with_activity.alias("s")
    .join(channel_stats.alias("g"), F.col("g.channel") == F.col("s.channel"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

with_parent_agent = (
    vs_group.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.median_csat",
        "s.csat_minus_median",
        "su.agent_name",
    )
)

screened = (
    with_parent_agent.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.channel") == "email"), "left_anti")
    .filter(F.month(F.col("closed_at")).isin(2, 3))
)

with_trends = (
    screened
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "agent_name",
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.lead("csat_minus_median", 1, 0).over(w_all_by_closed_at).alias("next_csat_minus_median"),
        F.lag("closed_at", 3).over(w_all_by_closed_at).alias("prev3_closed_at"),
        F.round(F.col("median_csat") / F.nullif(F.sum("median_csat").over(w_closed_at_month), F.lit(0)), 4).alias("median_csat_share_of_closed_at_month"),
        F.max("requester_id").over(w_all_by_closed_at_moving_2).alias("requester_id_max_3"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.min("event_at").alias("first_event_at"))
)

summarized_rows = (
    with_trends.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_event_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "agent_id",
        "priority",
        "agent_name",
        "minutes_since_prev_closed_at",
        "closed_at_percent_rank",
        "next_csat_minus_median",
        "prev3_closed_at",
        "median_csat_share_of_closed_at_month",
        "requester_id_max_3",
        "first_event_at",
        F.date_format(F.col("prev3_closed_at"), "yyyy-MM-dd").alias("prev3_closed_at_label"),
    )
    .orderBy(F.col("id").asc())
)
