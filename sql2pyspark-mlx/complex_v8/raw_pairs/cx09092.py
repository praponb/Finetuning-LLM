from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("opened_at") < F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
    )
)

ticket_side = recent.select("priority", "opened_at")

ticket_side_2 = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("csat") >= 2))
    .select("priority", F.col("closed_at").alias("opened_at"))
)

result = (
    ticket_side
    .unionByName(ticket_side_2)
    .distinct()
    .orderBy("opened_at")
)
