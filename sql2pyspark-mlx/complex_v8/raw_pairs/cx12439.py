from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_event_at_ties = Window.partitionBy("agent_id").orderBy("event_at")
w_agent_id_ticket_id_by_agent = Window.partitionBy("agent_id", "ticket_id").orderBy(F.col("agent_hired_on").desc(), "id")

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

with_details_agent = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

sequenced = (
    with_details_agent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "agent_name",
        "agent_hired_on",
        "agent_tier",
        F.round(F.percent_rank().over(w_agent_id_by_event_at_ties), 3).alias("event_at_percent_rank"),
        F.ntile(4).over(w_agent_id_ticket_id_by_agent).alias("agent_hired_on_quartile"),
        F.row_number().over(w_agent_id_ticket_id_by_agent).alias("seq_no"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "agent_id",
        "agent_name",
        "agent_hired_on",
        "event_at_percent_rank",
        "agent_hired_on_quartile",
        "seq_no",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.greatest(F.col("event_at_percent_rank"), F.col("agent_hired_on_quartile")).alias("greatest_event_at_percent_rank_agent_hired_on_qu"),
    )
    .orderBy(
        F.col("agent_name").asc_nulls_last(),
        F.col("event_at").asc_nulls_last(),
    )
)
