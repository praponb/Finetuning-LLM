from pyspark.sql import functions as F

event_side = (
    df_ticket_events
    .filter((F.col("event_type") != "escalated") & (F.col("actor").isNotNull()))
    .select(
        F.col("event_type").alias("label"),
        F.col("actor").alias("name"),
        F.col("id").alias("entity_id"),
    )
)

ticket_rows = df_tickets.select("priority", "channel", "csat")

ticket_rows_2 = (
    df_tickets
    .filter((F.col("channel") != "email") & (F.col("priority") == "P3"))
    .select("priority", "channel", "csat")
)

combined = (
    event_side
    .union(ticket_rows)
    .union(ticket_rows_2)
)

result = (
    combined
    .groupBy("label")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("label")
)
