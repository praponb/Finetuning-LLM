from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_counts = (
    filtered_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

derived = (
    with_counts
    .select(
        "*",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

with_activity_2 = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P4") & (F.col("t.requester_id") < 15), "left_semi")
)

result = (
    with_activity_2
    .rollup("event_type", "ticket_id", "actor")
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("actor") == "system", 1)).alias("n_actor_system"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "is_total_event_type",
        "cnt",
        "n_actor_system",
        "distinct_ticket_ids",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
