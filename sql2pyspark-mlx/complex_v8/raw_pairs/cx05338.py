from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter(F.col("team").isin("Tech", "Billing"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
    )
)

agent_side = (
    valid_agents
    .filter(F.col("agent_name").isNotNull())
    .select("id")
)

event_rows = df_ticket_events.select("id")

result = (
    agent_side
    .subtract(event_rows)
    .orderBy(F.col("id").desc())
)
