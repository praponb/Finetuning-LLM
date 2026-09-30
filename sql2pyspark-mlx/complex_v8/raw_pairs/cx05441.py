from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id_by_hours_s = Window.partitionBy("priority", "agent_id").orderBy(F.col("hours_since_open").desc(), "id")
w_all_by_hours_since_open = Window.orderBy(F.col("hours_since_open").desc(), "id")
w_priority_event_type = Window.partitionBy("priority", "event_type")
w_latest = Window.partitionBy("agent_id", "priority").orderBy(F.col("hours_since_open").desc(), F.col("id").desc())

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

windowed = (
    joined_ranges
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        (F.col("hours_since_open") - F.lag("hours_since_open").over(w_priority_agent_id_by_hours_s)).alias("hours_since_open_change"),
        F.row_number().over(w_all_by_hours_since_open).alias("rn"),
        F.round(F.col("hours_since_open") / F.nullif(F.sum("hours_since_open").over(w_priority_event_type), F.lit(0)), 4).alias("hours_since_open_share_of_priority_event_type"),
    )
)

with_details_agent = (
    windowed.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("F%")), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        "s.hours_since_open_change",
        "s.rn",
        "s.hours_since_open_share_of_priority_event_type",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

unique_rows = (
    with_details_agent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .filter(F.length(F.trim(F.col("agent_name"))) >= 7)
    .select(
        "id",
        "event_type",
        "priority",
        "agent_id",
        "hours_since_open",
        "rn",
        "hours_since_open_share_of_priority_event_type",
        "agent_name",
        "ticket_tags",
        "ticket_closed_at",
    )
    .orderBy(F.col("ticket_closed_at").desc())
)
