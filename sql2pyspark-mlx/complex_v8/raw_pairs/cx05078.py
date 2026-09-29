from pyspark.sql import functions as F

base = (
    df_tickets
    .filter(F.month(F.col("closed_at")).isin(1, 2))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.initcap(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

ticket_rows = (
    base
    .select(
        F.lit("ticket").alias("src"),
        "channel_clean",
        F.col("opened_at").alias("happened_at"),
    )
)

event_rows = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 52)
    .select(
        F.lit("event").alias("src"),
        F.col("event_type").alias("channel_clean"),
        F.col("event_at").alias("happened_at"),
    )
)

event_rows_2 = (
    df_ticket_events
    .select(
        F.lit("event").alias("src"),
        F.col("actor").alias("channel_clean"),
        F.col("event_at").alias("happened_at"),
    )
)

combined = (
    ticket_rows
    .union(event_rows)
    .union(event_rows_2)
)

result = (
    combined
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("src")
)
