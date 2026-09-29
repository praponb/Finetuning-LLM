from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter((~F.col("channel").isin("email", "web")) & (F.col("requester_id") < 22))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
    )
)

ticket_rows = (
    prepared
    .filter(F.col("channel") != "email")
    .select("requester_id", F.col("agent_id").alias("entity_id"))
)

event_side = (
    df_ticket_events
    .filter(F.col("actor") == "agent")
    .select(
        F.col("ticket_id").alias("requester_id"),
        F.col("id").alias("entity_id"),
    )
)

event_side_2 = (
    df_ticket_events
    .select(
        F.col("id").alias("requester_id"),
        F.col("ticket_id").alias("entity_id"),
    )
)

combined = (
    ticket_rows
    .unionByName(event_side)
    .unionByName(event_side_2)
)

result = (
    combined
    .groupBy("requester_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("requester_id")
)
