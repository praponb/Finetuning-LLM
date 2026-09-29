from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .select(
        F.col("ticket_id").alias("ref_id"),
        F.col("actor").alias("name"),
        F.lit("event").alias("source"),
    )
)

agent_rows = df_support_agents.select("id", "team", F.lit("agent").alias("source"))

agent_side = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("L%")) & (F.year(F.col("hired_on")) == 2024))
    .select("id", "agent_name", F.lit("agent").alias("source"))
)

result = (
    event_rows
    .union(agent_rows)
    .union(agent_side)
)
