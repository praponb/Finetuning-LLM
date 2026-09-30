from pyspark.sql import functions as F

scoped_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-15 08:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
)

derived = (
    scoped_tickets
    .filter(F.col("channel_or_unknown") == "email")
    .select(
        "*",
        F.when(F.col("priority_len") >= 9, "large").when(F.col("priority_len") >= 6, "medium").otherwise("small").alias("priority_len_tier"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

result = (
    derived
    .select(
        "id",
        "csat",
        "priority_len",
        "requester_id",
    )
    .unpivot(
        ids=["id"],
        values=["csat", "priority_len", "requester_id"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
