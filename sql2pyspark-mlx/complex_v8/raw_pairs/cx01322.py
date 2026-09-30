from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(6, 10))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

scored = (
    prepared
    .select(
        "*",
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

result = (
    scored
    .groupingSets(
        [["actor", "event_type", "ticket_id"], ["actor"], ["event_type"]],
        "actor", "event_type", "ticket_id",
    )
    .agg(F.max("event_at").alias("last_event_at"))
)
