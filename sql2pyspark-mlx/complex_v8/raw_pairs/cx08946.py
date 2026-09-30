from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_csat_ticket_id = Window.partitionBy("ticket_csat", "ticket_id")
w_ticket_csat_ticket_id_by_age = Window.partitionBy("ticket_csat", "ticket_id").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_ticket_id_by_age_2 = Window.partitionBy("ticket_csat", "ticket_id").orderBy("agent_hired_on", "id")

cleaned_events = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("agent", "customer"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.requester_id") >= 11)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

joined_ticket = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.closed_at")).between(6, 10)), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .filter(F.col("t.csat") > 3)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.ticket_requester_id",
        "s.agent_hired_on",
        F.col("t.csat").alias("ticket_csat"),
        "su.agent_name",
    )
)

with_trends = (
    joined_ticket
    .select(
        "*",
        F.count(F.lit(1)).over(w_ticket_csat_ticket_id).alias("rows_in_ticket_csat_ticket_id"),
        F.last("agent_hired_on").over(w_ticket_csat_ticket_id_by_age).alias("last_agent_hired_on"),
        F.rank().over(w_ticket_csat_ticket_id_by_age_2).alias("agent_hired_on_rank"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "ticket_requester_id",
        "agent_hired_on",
        "ticket_csat",
        "agent_name",
        "last_agent_hired_on",
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
    .orderBy(F.col("ticket_id").desc())
)
