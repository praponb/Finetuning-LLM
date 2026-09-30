from pyspark.sql import functions as F

ticket_side = (
    df_tickets
    .filter(~F.col("channel").isin("web", "chat"))
    .select(
        F.col("priority").alias("name"),
        "opened_at",
        F.col("agent_id").alias("qty"),
        F.lit("ticket").alias("source"),
    )
)

event_rows = (
    df_ticket_events
    .select(
        "event_type",
        "event_at",
        "id",
        F.lit("event").alias("source"),
    )
)

all_events = ticket_side.union(event_rows)

result = (
    all_events
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("qty").alias("total_qty"),
        F.max("opened_at").alias("latest_opened_at"),
    )
    .orderBy("source")
)
