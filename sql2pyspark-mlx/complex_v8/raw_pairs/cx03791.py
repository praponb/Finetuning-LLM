from pyspark.sql import functions as F

prepared = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

screened = (
    prepared.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-01-31").cast("date"))
)

grouped = (
    screened
    .filter(F.col("event_type_ref").isNotNull())
    .groupBy("actor", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.countDistinct("id").alias("distinct_ids"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.count(F.when(F.col("event_type") == "created", 1)).alias("n_event_type_created"),
    )
    .filter(F.col("row_count") >= 3)
)

result = (
    grouped
    .select(
        "actor",
        "event_at_month",
        "row_count",
        "distinct_ids",
        "actor_list",
        "n_event_type_created",
    )
    .orderBy(
        F.col("n_event_type_created").asc(),
        F.col("actor").asc(),
        "event_at_month",
    )
    .limit(20)
)
