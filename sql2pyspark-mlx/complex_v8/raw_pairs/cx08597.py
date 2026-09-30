from pyspark.sql import functions as F

filtered = (
    df_tickets
    .filter(F.to_date(F.col("opened_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 22, "large").when(F.col("requester_id") >= 9, "medium").otherwise("small").alias("requester_id_tier"),
        F.when(F.col("requester_id") >= 19, "large").when(F.col("requester_id") >= 7, "medium").otherwise("small").alias("requester_id_tier_2"),
    )
)

without_matches = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isin("P4", "P1")), "left_anti")
)

result = (
    without_matches
    .select("id", "csat", "requester_id")
    .unpivot(
        ids=["id"],
        values=["csat", "requester_id"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
