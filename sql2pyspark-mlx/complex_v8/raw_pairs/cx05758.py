from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter((~F.col("priority").isin("P2", "P1")) & (F.col("csat") >= 3))
    .select("csat", "channel")
)

agent_side = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("% n%"))
    .select("id", "tier")
)

result = (
    ticket_rows
    .intersect(agent_side)
    .orderBy("csat", "channel")
)
