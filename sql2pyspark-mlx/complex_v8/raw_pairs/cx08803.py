from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .filter((~F.col("event_type").isin("assigned", "escalated")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_type",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

totals = (
    enriched
    .filter(F.col("event_type") == "created")
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(~F.col("event_type").isin("replied", "assigned"), 1)).alias("n_event_type_not_in_replied_as"),
        F.count(F.when(F.col("event_type").isNotNull(), 1)).alias("n_event_type_is_not_null"),
    )
)

with_activity = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp")), "left_anti")
)

result = (
    with_activity
    .select(
        "ticket_id",
        "cnt",
        "distinct_ticket_ids",
        "n_event_type_not_in_replied_as",
        "n_event_type_is_not_null",
    )
    .orderBy(
        F.col("n_event_type_is_not_null").asc_nulls_last(),
        F.col("n_event_type_not_in_replied_as").desc(),
    )
)
