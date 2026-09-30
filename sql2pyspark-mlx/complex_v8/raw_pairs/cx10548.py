from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) | (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

exploded_detail_events = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

summary = (
    exploded_detail_events
    .filter(F.col("ticket_id") <= 52)
    .groupBy(
        "ticket_id",
        "event_actor",
        F.to_date("event_event_at").alias("event_event_at_day"),
    )
    .agg(
        F.max("event_event_at").alias("last_event_event_at"),
        F.sum(F.when(~F.col("event_type").isin("reopened", "escalated"), 1).otherwise(0)).alias("n_event_type_not_in_reopened_e"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    summary
    .select(
        "ticket_id",
        "event_actor",
        "last_event_event_at",
        "n_event_type_not_in_reopened_e",
        "event_type_list",
        "n_rows",
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("last_event_event_at").desc(),
    )
)
