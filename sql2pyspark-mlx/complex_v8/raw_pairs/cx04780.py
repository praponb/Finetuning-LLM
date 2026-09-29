from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .select(F.col("actor").alias("label"), "event_type", F.lit("event").alias("src"))
)

agent_side = df_support_agents.select("team", "agent_name", F.lit("agent").alias("src"))

agent_rows = (
    df_support_agents
    .filter((F.col("tier") == "T1") & (F.upper(F.trim(F.col("agent_name"))).like("K%")))
    .select("agent_name", "tier", F.lit("agent").alias("src"))
)

stacked = (
    event_rows
    .union(agent_side)
    .union(agent_rows)
)

result = (
    stacked
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("src")
)
