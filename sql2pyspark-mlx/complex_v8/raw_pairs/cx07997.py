from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_month_ties = Window.orderBy(F.col("event_at_month").desc())
w_actor_ticket_agent_id_by_age = Window.partitionBy("actor", "ticket_agent_id").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(-6, -1)

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id") <= 3) & (F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.month(F.col("event_at")).alias("event_at_month"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.size(F.col("t.tags")) >= 1), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "event_at_month",
        "agent_hired_on",
        F.round(F.cume_dist().over(w_all_by_event_at_month_ties), 3).alias("event_at_month_cume_dist"),
        F.round(F.avg("event_at_month").over(w_actor_ticket_agent_id_by_age), 2).alias("event_at_month_avg_6_prior"),
    )
)

result = (
    ranked
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "event_at",
        "event_type",
        "actor_slug",
        "event_at_month_cume_dist",
        "event_at_month_avg_6_prior",
    )
    .orderBy("id", "event_at")
)
