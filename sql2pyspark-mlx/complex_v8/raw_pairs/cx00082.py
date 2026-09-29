from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

combined_details = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.agent_id").alias("ticket_agent_id"))
)

annotated = (
    combined_details
    .select(
        "*",
        F.concat_ws(" / ", F.col("event_at_label"), F.col("actor")).alias("event_at_label_actor_label"),
    )
)

result = (
    annotated
    .rollup("actor", "ticket_agent_id")
    .agg(
        F.count(F.when(F.col("actor") == "system", 1)).alias("n_actor_system"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("ticket_agent_id").asc_nulls_last(),
    )
)
