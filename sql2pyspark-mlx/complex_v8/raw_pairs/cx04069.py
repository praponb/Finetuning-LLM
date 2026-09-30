from pyspark.sql import functions as F

base_agents = (
    df_support_agents
    .filter(F.col("team") == "Onboarding")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.concat(F.col("team"), F.lit("#"), F.col("id").cast("string")).alias("team_ref"),
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
    )
)

labeled = (
    base_agents
    .select(
        "*",
        F.abs(F.col("hired_on_quarter") - 2).alias("hired_on_quarter_distance"),
        F.lower(F.trim(F.col("agent_name"))).alias("agent_name_clean"),
    )
)

with_children = (
    labeled.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.requester_id") > 7), "inner")
    .select(
        "s.id",
        "s.agent_name",
        "s.team",
        "s.tier",
        "s.hired_on_quarter",
        "s.agent_name_clean",
        F.col("t.id").alias("ticket_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    with_children
    .groupBy("tier")
    .pivot("ticket_channel", ["web", "phone", "email", "chat"])
    .agg(F.sum("hired_on_quarter").alias("amount"), F.count("hired_on_quarter").alias("n"))
    .orderBy("tier")
)
