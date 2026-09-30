from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(~F.col("event_type").isin("resolved", "assigned"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
)

enriched_ticket = (
    cleaned.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.csat") > 1), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        F.col("t.tags").alias("ticket_tags"),
    )
)

scored = enriched_ticket.withColumn("ticket_tags_count", F.size(F.col("ticket_tags")))

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "actor",
        "ticket_tags_count",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
    .orderBy(F.col("ticket_id").desc_nulls_first())
)
