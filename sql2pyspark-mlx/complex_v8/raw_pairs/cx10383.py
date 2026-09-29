from pyspark.sql import functions as F

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 7 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

per_group = (
    in_window
    .filter(F.col("hours_since_open") >= 25.2)
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
        F.count(F.when(F.col("event_type").isin("replied", "reopened", "created"), 1)).alias("n_event_type_in_replied_reopen"),
    )
)

qualified = (
    per_group
    .crossJoin(per_group.agg(F.avg("n_event_type_is_not_null").alias("overall_avg_n_event_type_is_not_null")))
    .filter(F.col("n_event_type_is_not_null") > F.col("overall_avg_n_event_type_is_not_null"))
    .drop("overall_avg_n_event_type_is_not_null")
)

result = (
    qualified
    .select(
        "ticket_id",
        "n_event_type_is_not_null",
        "n_event_type_in_replied_reopen",
        F.month(F.col("first_event_at")).alias("first_event_at_month"),
    )
    .orderBy(F.col("n_event_type_is_not_null").desc(), "ticket_id")
    .limit(5)
)
