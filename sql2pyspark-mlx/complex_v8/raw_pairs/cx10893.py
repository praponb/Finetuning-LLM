from pyspark.sql import functions as F

joined_ranges = (
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

with_children_events = (
    joined_ranges.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_1d",
        "s.prior_requester_id_1d",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_type",
    )
)

screened = (
    with_children_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.priority").isin("P2", "P1", "P3")), "left_anti")
    .filter(F.col("event_id") <= 28)
)

result = (
    screened
    .select(
        "id",
        "opened_at",
        "prior_rows_1d",
        "prior_requester_id_1d",
        "event_id",
        "event_actor",
        F.initcap(F.trim(F.col("event_actor"))).alias("event_actor_clean"),
    )
    .orderBy(F.col("event_id").asc(), "id")
)
