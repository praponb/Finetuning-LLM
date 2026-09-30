from pyspark.sql import functions as F

ticket_rows = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-26 16:00:00").cast("timestamp"))
    .select(
        F.lit("ticket").alias("record_type"),
        "csat",
        "priority",
        F.col("closed_at").alias("happened_at"),
    )
)

event_rows = (
    df_ticket_events
    .filter(F.col("event_type").isin("resolved", "replied"))
    .select(
        F.lit("event").alias("record_type"),
        F.col("id").alias("csat"),
        F.col("actor").alias("priority"),
        F.col("event_at").alias("happened_at"),
    )
)

stacked = (
    ticket_rows
    .unionByName(event_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("record_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("csat").alias("total_csat"),
        F.max("happened_at").alias("latest_happened_at"),
    )
    .orderBy("record_type")
)
