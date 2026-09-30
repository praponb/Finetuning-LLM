from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("event_type_group").orderBy(F.col("event_at").desc(), F.col("id").desc())

base_events = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.when(F.col("event_type").isin("escalated", "created"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
    )
)

latest = (
    base_events
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

screened = (
    latest.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority").isNotNull()), "left_anti")
)

with_parent_ticket = (
    screened.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

result = (
    with_parent_ticket
    .filter(F.col("agent_tier").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "event_at_date",
        "ticket_requester_id",
        "ticket_agent_id",
        "agent_tier",
        F.length(F.trim(F.col("agent_tier"))).alias("agent_tier_len"),
        F.coalesce(F.col("event_type_group"), F.lit("UNKNOWN")).alias("event_type_group_or_unknown"),
    )
    .orderBy("event_at_date", F.col("ticket_id").asc_nulls_last(), "id")
    .limit(10)
)
