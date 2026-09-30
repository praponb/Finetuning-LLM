from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_event_at = Window.partitionBy("priority").orderBy(F.col("event_at").desc_nulls_first(), "id")
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_agent_id = Window.partitionBy("event_type", "agent_id")

covered = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

with_windows = (
    covered
    .select(
        "*",
        F.rank().over(w_priority_by_event_at).alias("event_at_rank"),
        F.nth_value("hours_since_open", 2).over(w_ticket_id_by_event_at_full).alias("second_hours_since_open"),
        F.round(F.col("hours_since_open") / F.nullif(F.sum("hours_since_open").over(w_event_type_agent_id), F.lit(0)), 4).alias("hours_since_open_share_of_event_type_agent_id"),
        F.sum("hours_since_open").over(w_event_type_agent_id).alias("event_type_agent_id_sum_hours_since_open"),
        F.ntile(4).over(w_priority_by_event_at).alias("event_at_quartile"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.csat") > 3)
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_type",
        "agent_id",
        "event_at_rank",
        "second_hours_since_open",
        "event_type_agent_id_sum_hours_since_open",
        "event_at_quartile",
        "ticket_csat",
        "agent_team",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
        F.abs(F.col("event_at_rank") - 6).alias("event_at_rank_distance"),
    )
    .orderBy(F.col("ticket_csat").desc(), F.col("event_type").asc())
)
