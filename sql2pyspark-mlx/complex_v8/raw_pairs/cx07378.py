from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_type_ref",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

result = (
    with_details
    .cube("ticket_id", "agent_tier")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(100 * F.count(F.when(F.col("event_type") == "reopened", 1)) / F.count("*"), 1).alias("pct_event_type_reopened"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("agent_tier").asc_nulls_last(),
    )
)
