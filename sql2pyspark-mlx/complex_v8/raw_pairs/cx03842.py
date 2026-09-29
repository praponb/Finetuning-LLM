from pyspark.sql import functions as F

recent = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
    )
)

ticket_side = (
    recent
    .filter(F.month(F.col("closed_at")).isin(1, 3))
    .select(F.lit("ticket").alias("source"), F.col("id").alias("key_id"), "agent_id")
)

event_side = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 43)
    .select(F.lit("event").alias("source"), "id", "ticket_id")
)

unioned = (
    ticket_side
    .union(event_side)
    .distinct()
)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("source")
)
