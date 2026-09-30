from pyspark.sql import functions as F

ticket_rows = df_tickets.select(F.col("priority").alias("category"), F.col("csat").alias("entity_id"))

event_side = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 50) & (F.col("actor").isin("agent", "system")))
    .select("actor", "ticket_id")
)

combined = (
    ticket_rows
    .union(event_side)
    .distinct()
)

result = (
    combined
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("category")
)
