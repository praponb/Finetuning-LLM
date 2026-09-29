from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

without_matches = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
    .filter(F.col("agent_id").isNotNull())
)

result = (
    without_matches
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_12h",
        "prior_requester_id_12h",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)
