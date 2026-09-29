from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.col("csat") > 2) & (F.col("priority").isin("P2", "P4", "P3")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_activity = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") < 4), "left_semi")
)

result = (
    with_activity
    .cube("priority", "agent_id")
    .agg(F.max("csat").alias("max_csat"))
    .orderBy(F.col("priority").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
