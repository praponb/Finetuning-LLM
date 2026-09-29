from pyspark.sql import functions as F

base = (
    df_support_agents
    .filter(F.col("hired_on") < F.lit("2024-12-31").cast("date"))
    .select(
        "id",
        "agent_name",
        "team",
        "hired_on",
        F.when(F.col("tier").isin("T2", "T1"), "group_a").when(F.col("tier") == "T3", "group_b").otherwise("other").alias("tier_group"),
    )
)

agent_side = base.select("id", "team")

ticket_side = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-02-06").cast("date")) & (F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp")))
    .select("csat", "channel")
)

result = (
    agent_side
    .intersectAll(ticket_side)
    .orderBy("team", "id")
)
