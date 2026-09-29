from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.col("csat") > 2)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

result = (
    cleaned_tickets
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "tags",
    )
)
