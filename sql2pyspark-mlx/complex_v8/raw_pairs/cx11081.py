from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("id").cast("string")).alias("event_type_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

qualified = (
    base_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "created") & (F.col("t.actor").isin("customer", "agent")), "left_anti")
    .filter(F.col("event_type_ref").isNotNull())
)

result = (
    qualified
    .cube("ticket_id", "actor", "event_type")
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("n_event_type_assigned"),
        F.sum(F.when(F.col("actor") != "customer", 1).otherwise(0)).alias("n_actor_customer"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
