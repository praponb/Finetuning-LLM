from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("event_type") == "escalated")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

enriched = (
    prepared_events
    .withColumn("greatest_event_at_dayofweek_actor_len", F.greatest(F.col("event_at_dayofweek"), F.col("actor_len")))
)

exploded_detail_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.greatest_event_at_dayofweek_actor_len",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

screened = (
    exploded_detail_events
    .crossJoin(exploded_detail_events.agg(F.max("greatest_event_at_dayofweek_actor_len").alias("overall_max_greatest_event_at_dayofweek_actor_le")))
    .filter(F.col("greatest_event_at_dayofweek_actor_len") < F.col("overall_max_greatest_event_at_dayofweek_actor_le"))
    .drop("overall_max_greatest_event_at_dayofweek_actor_le")
)

result = (
    screened
    .select(
        "id",
        "ticket_id",
        "event_at",
        "greatest_event_at_dayofweek_actor_len",
        "event_id",
        "event_event_at",
        "event_event_type",
        "event_actor",
        F.coalesce(F.col("event_actor"), F.lit("UNKNOWN")).alias("event_actor_or_unknown"),
    )
    .orderBy(F.col("event_event_at").asc(), F.col("event_actor").asc())
)
