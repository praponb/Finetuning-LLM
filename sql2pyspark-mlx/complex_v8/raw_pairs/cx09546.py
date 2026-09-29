from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(9, 14))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
    )
)

derived = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

tagged = (
    derived
    .select(
        "id",
        "opened_at",
        "csat_per_requester_id",
        "csat_filled",
        F.abs(F.col("csat_per_requester_id") - 5.5).alias("csat_per_requester_id_distance"),
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select("s.*", "t.last_event_at", "t.first_event_at")
)

result = (
    profiled
    .select("id", "csat_per_requester_id", "csat_per_requester_id_distance")
    .unpivot(
        ids=["id"],
        values=["csat_per_requester_id", "csat_per_requester_id_distance"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
