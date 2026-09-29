from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.requester_id").alias("prior_requester_id_7d"),
    )
)

exploded_detail = (
    covered.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_7d",
        "s.prior_requester_id_7d",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "event_id",
        "prior_rows_7d",
        "prior_requester_id_7d",
    )
    .unpivot(
        ids=["id", "event_id"],
        values=["prior_rows_7d", "prior_requester_id_7d"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "event_id", "measure")
)
