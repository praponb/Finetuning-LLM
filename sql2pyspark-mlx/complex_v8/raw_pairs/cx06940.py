from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("channel").isNotNull())
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
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

ticket_side = (
    filtered_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-18 13:00:00").cast("timestamp"))
    .select(F.col("priority").alias("category"))
)

ticket_side_2 = (
    df_tickets
    .filter(F.col("channel").isin("web", "chat"))
    .select("channel")
)

result = (
    ticket_side
    .exceptAll(ticket_side_2)
    .orderBy(F.col("category").desc())
)
