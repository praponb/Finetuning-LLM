from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter((F.col("hired_on") >= F.lit("2024-07-28").cast("date")) & (F.upper(F.trim(F.col("agent_name"))).like("K%")))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.concat(F.col("agent_name"), F.lit("#"), F.col("id").cast("string")).alias("agent_name_ref"),
        F.add_months(F.col("hired_on"), 3).alias("hired_on_plus_3m"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("closed_at").alias("last_ticket_closed_at"))
)

with_counts = (
    scoped.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_ticket_closed_at")
)

result = (
    with_counts
    .select(
        "id",
        "agent_name",
        "hired_on",
        "agent_name_ref",
        "last_ticket_closed_at",
        F.datediff(F.col("hired_on"), F.col("hired_on_plus_3m")).alias("days_hired_on_plus_3m_to_hired_on"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_ticket_closed_at")).alias("days_since_last_ticket_closed_at"),
    )
    .orderBy(F.col("hired_on").desc())
)
