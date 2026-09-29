from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter(F.col("requester_id").between(3, 18))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

scored = (
    cleaned
    .filter(F.col("channel") != "chat")
    .select(
        "*",
        F.upper(F.trim(F.col("priority"))).alias("priority_clean"),
        F.lower(F.trim(F.col("priority"))).alias("priority_clean_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "priority_clean",
        "priority_clean_2",
    )
    .orderBy(F.col("priority").desc())
)
