from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_ticket_opened_at_month = Window.partitionBy("actor", F.month("ticket_opened_at"))
w_agent_tier_ticket_id_by_tick = Window.partitionBy("agent_tier", "ticket_id").orderBy("ticket_closed_at", "id")

filtered_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

joined_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.closed_at") >= F.lit("2025-01-20 22:00:00").cast("timestamp")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.tier").alias("agent_tier"),
    )
)

ranked = (
    joined_ticket
    .select(
        "*",
        F.count(F.lit(1)).over(w_actor_ticket_opened_at_month).alias("rows_in_actor_ticket_opened_at_month"),
        F.ntile(3).over(w_agent_tier_ticket_id_by_tick).alias("ticket_closed_at_tercile"),
        F.min("ticket_csat").over(w_actor_ticket_opened_at_month).alias("actor_ticket_opened_at_month_min_ticket_csat"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_type_clean",
        "ticket_csat",
        "ticket_opened_at",
        "agent_tier",
        "rows_in_actor_ticket_opened_at_month",
        "actor_ticket_opened_at_month_min_ticket_csat",
    )
    .orderBy(F.col("event_type_clean").desc(), F.col("actor").desc_nulls_first())
)
