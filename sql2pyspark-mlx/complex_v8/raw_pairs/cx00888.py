from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter(F.col("requester_id") >= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

result = (
    prepared_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
    )
)
