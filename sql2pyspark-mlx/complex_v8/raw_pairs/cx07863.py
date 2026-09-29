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

with_activity = (
    covered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

annotated = (
    with_activity
    .select(
        "*",
        F.date_format(F.col("opened_at"), "MMM yyyy").alias("opened_at_label"),
    )
)

exploded_detail = (
    annotated.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.prior_rows_7d",
        "s.prior_requester_id_7d",
        "s.opened_at_label",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "opened_at",
        "prior_rows_7d",
        "opened_at_label",
        "event_id",
        "event_type",
        "event_at",
        "event_actor",
        F.abs(F.col("prior_rows_7d") - 4).alias("prior_rows_7d_distance"),
        F.lower(F.trim(F.col("opened_at_label"))).alias("opened_at_label_clean"),
    )
)
