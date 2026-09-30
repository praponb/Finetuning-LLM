from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor") != "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

grouped = (
    base
    .groupBy("event_type", "event_type_or_unknown", "ticket_id")
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    grouped
    .groupBy("ticket_id", "event_type_or_unknown")
    .pivot("event_type", ["resolved", "escalated"])
    .agg(F.sum("n_rows").alias("amount"), F.max("n_rows").alias("max_v"))
    .orderBy("ticket_id", "event_type_or_unknown")
)
