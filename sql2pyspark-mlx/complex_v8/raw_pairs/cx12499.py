from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_ticket_opened_at = Window.partitionBy("agent_id").orderBy(F.col("ticket_opened_at").desc())
w_agent_id_by_ticket_opened_at_2 = Window.partitionBy("agent_id").orderBy(F.col("ticket_opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_ticket_opened_at_3 = Window.partitionBy("agent_id").orderBy(F.col("ticket_opened_at").desc(), "id")

matched = (
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

enriched = (
    matched.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("F%"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.priority",
        "s.agent_id",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.round(F.cume_dist().over(w_agent_id_by_ticket_opened_at), 3).alias("ticket_opened_at_cume_dist"),
        F.round(F.percent_rank().over(w_agent_id_by_ticket_opened_at), 3).alias("ticket_opened_at_percent_rank"),
        F.first("ticket_opened_at").over(w_agent_id_by_ticket_opened_at_2).alias("first_ticket_opened_at"),
        F.ntile(5).over(w_agent_id_by_ticket_opened_at_3).alias("ticket_opened_at_quintile"),
    )
)

result = (
    sequenced
    .select("id", "ticket_opened_at_percent_rank", "ticket_opened_at_cume_dist")
    .unpivot(
        ids=["id"],
        values=["ticket_opened_at_percent_rank", "ticket_opened_at_cume_dist"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
