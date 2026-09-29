from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_agent_id_by = Window.partitionBy("ticket_id", "ticket_agent_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")) | (~F.col("event_type").isin("reopened", "escalated")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

enriched = (
    recent_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.first("event_at", ignorenulls=True).over(w_ticket_id_ticket_agent_id_by).alias("first_known_event_at"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_slug",
        "agent_tier",
        "agent_name",
        F.when(F.col("event_type").isin("created", "replied"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.length(F.trim(F.col("actor_slug"))).alias("actor_slug_len"),
    )
)
