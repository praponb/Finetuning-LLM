from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type").isin("replied", "assigned", "resolved")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

scored = (
    filtered
    .withColumn("event_type_group", F.when(F.col("event_type").isin("assigned", "escalated"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other"))
)

result = (
    scored
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_or_unknown",
        "event_type_group",
    )
    .orderBy(F.col("actor_or_unknown").desc_nulls_first())
)
