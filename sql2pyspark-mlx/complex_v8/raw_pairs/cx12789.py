from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-10 21:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

screened = (
    cleaned_tickets.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-06").cast("date")), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["agent_id", "channel", "priority"], ["agent_id"]],
        "agent_id", "channel", "priority",
    )
    .agg(F.sum("csat").alias("total_csat"))
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
        F.col("priority").asc_nulls_last(),
    )
)
