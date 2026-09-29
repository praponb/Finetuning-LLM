from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((~F.col("event_type").isin("escalated", "reopened")) & (F.month(F.col("event_at")).isin(1, 2)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

combined_details_ticket = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_hour",
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

stats = (
    combined_details_ticket
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("n_actor_system"),
        F.count(F.when(F.col("event_type").isin("replied", "escalated", "resolved"), 1)).alias("n_event_type_in_replied_escala"),
        F.count("*").alias("row_count"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.sum(F.when(F.col("event_type") == "reopened", 1).otherwise(0)).alias("n_event_type_reopened"),
    )
)

result = (
    stats
    .select("ticket_id", "row_count", "n_event_type_in_replied_escala")
    .unpivot(
        ids=["ticket_id"],
        values=["row_count", "n_event_type_in_replied_escala"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("ticket_id", "field")
)
