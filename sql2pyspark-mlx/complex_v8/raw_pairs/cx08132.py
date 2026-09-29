from pyspark.sql import functions as F

recent = (
    df_support_agents
    .filter((F.col("team").isNotNull()) & (F.col("tier") != "T3"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.upper(F.trim(F.col("team"))).alias("team_clean"),
    )
)

agent_side = recent.select(F.col("id").alias("ref_id"))

event_rows = df_ticket_events.select(F.col("ticket_id").alias("ref_id"))

result = (
    agent_side
    .subtract(event_rows)
    .orderBy("ref_id")
)
