from pyspark.sql import functions as F

recent = (
    df_support_agents
    .filter(F.lower(F.col("agent_name")).like("%wa%"))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

agent_side = (
    recent
    .select(
        F.col("id").alias("key_id"),
        F.col("team_slug").alias("label"),
        F.lit("agent").alias("origin"),
    )
)

ticket_rows = df_tickets.select("id", "channel", F.lit("ticket").alias("origin"))

result = agent_side.union(ticket_rows)
