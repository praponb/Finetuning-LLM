from pyspark.sql import functions as F

event_rows = (
    df_ticket_events
    .filter((F.col("event_type") == "reopened") & (F.hour(F.col("event_at")).between(6, 10)))
    .select(F.lit("event").alias("origin"), "ticket_id", "id")
)

ticket_rows = df_tickets.select(F.lit("ticket").alias("origin"), "csat", "requester_id")

ticket_rows_2 = df_tickets.select(F.lit("ticket").alias("origin"), "agent_id", "csat")

all_events = (
    event_rows
    .union(ticket_rows)
    .union(ticket_rows_2)
)

result = (
    all_events
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("id").alias("total_id"))
    .orderBy("origin")
)
