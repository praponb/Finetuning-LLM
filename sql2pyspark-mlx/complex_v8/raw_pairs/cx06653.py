from pyspark.sql import functions as F

ticket_side = df_tickets.select(F.col("id").alias("entity_id"), "closed_at", "agent_id")

event_rows = df_ticket_events.select("ticket_id", "event_at", "id")

combined = ticket_side.union(event_rows)

result = (
    combined
    .groupBy("entity_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("agent_id").alias("total_agent_id"),
        F.max("closed_at").alias("latest_closed_at"),
    )
    .orderBy("entity_id")
)
