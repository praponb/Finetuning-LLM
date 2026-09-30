from pyspark.sql import functions as F

recent = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "outage")) & (F.col("opened_at") >= F.lit("2025-01-19 10:00:00").cast("timestamp")))
)

annotated = (
    recent
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        (F.col("opened_at") + F.expr("INTERVAL 1 DAYS")).alias("opened_at_plus_1d"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

screened = (
    annotated.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Tech"), "left_semi")
)

result = (
    screened
    .select("id", "csat", "requester_id")
    .unpivot(
        ids=["id"],
        values=["csat", "requester_id"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
