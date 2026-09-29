from pyspark.sql import functions as F

base_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

result = (
    base_tickets
    .filter(F.col("agent_id").isNotNull())
    .select("id", "agent_id", "channel")
    .orderBy(F.col("id").asc(), "agent_id")
)
