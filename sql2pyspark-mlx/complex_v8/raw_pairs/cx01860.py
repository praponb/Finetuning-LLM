from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("closed_at") >= F.lit("2025-01-12 08:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 60, 1).alias("minutes_opened_at_to_closed_at"),
    )
)

exploded = (
    valid_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "opened_at_quarter",
        "minutes_opened_at_to_closed_at",
        F.posexplode_outer("tags").alias("tag_pos", "tag"),
    )
)

result = (
    exploded
    .select(
        "id",
        "tag_pos",
        "requester_id",
        "opened_at_quarter",
        "csat",
    )
    .unpivot(
        ids=["id", "tag_pos"],
        values=["requester_id", "opened_at_quarter", "csat"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
