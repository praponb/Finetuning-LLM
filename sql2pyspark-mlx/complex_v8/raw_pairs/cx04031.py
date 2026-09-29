from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.requester_id").alias("prior_requester_id_1d"),
    )
)

annotated = (
    covered
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_1d",
        F.least(F.col("prior_requester_id_1d"), F.col("prior_rows_1d")).alias("least_prior_requester_id_1d_prior_rows_1d"),
        F.greatest(F.col("prior_requester_id_1d"), F.col("prior_rows_1d")).alias("greatest_prior_requester_id_1d_prior_rows_1d"),
        F.round(F.col("prior_rows_1d") / F.nullif(F.col("prior_requester_id_1d"), F.lit(0)), 3).alias("prior_rows_1d_per_prior_requester_id_1d"),
    )
)

exploded_detail_events = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.least_prior_requester_id_1d_prior_rows_1d",
        "s.greatest_prior_requester_id_1d_prior_rows_1d",
        "s.prior_rows_1d_per_prior_requester_id_1d",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail_events
    .groupingSets(
        [["event_type", "agent_id", "event_id"], ["event_type"], ["agent_id"], []],
        "event_type", "agent_id", "event_id",
    )
    .agg(
        F.min("opened_at").alias("first_opened_at"),
        F.round(F.sum("greatest_prior_requester_id_1d_prior_rows_1d") / F.nullif(F.sum("least_prior_requester_id_1d_prior_rows_1d"), F.lit(0)), 3).alias("greatest_prior_requester_id_1d_prior_rows_1d_to_"),
        F.count("*").alias("cnt"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "agent_id",
        "event_id",
        "first_opened_at",
        "greatest_prior_requester_id_1d_prior_rows_1d_to_",
        "cnt",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("event_id").asc_nulls_last(),
    )
)
