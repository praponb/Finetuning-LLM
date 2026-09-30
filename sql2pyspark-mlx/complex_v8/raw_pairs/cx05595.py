from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(~F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

profiled = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

combined_details = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.requester_id") <= 20)
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.first_event_at",
        "s.last_event_at",
        "s.event_count",
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

result = (
    combined_details
    .select("id", "event_count", "ticket_requester_id")
    .unpivot(
        ids=["id"],
        values=["event_count", "ticket_requester_id"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
