from pyspark.sql import functions as F

base_agents = (
    df_support_agents
    .filter((F.upper(F.trim(F.col("agent_name"))).like("F%")) & (F.col("hired_on") >= F.lit("2024-07-09").cast("date")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
    )
)

with_activity = (
    base_agents.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.requester_id").between(2, 25)) & (F.col("t.opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")), "left_semi")
)

with_activity_2 = (
    with_activity.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (F.col("t.csat") > 3) & (F.col("t.channel") == "phone"), "left_anti")
)

result = (
    with_activity_2
    .select("id", "agent_name", "tier")
    .orderBy(F.col("agent_name").asc_nulls_last())
)
