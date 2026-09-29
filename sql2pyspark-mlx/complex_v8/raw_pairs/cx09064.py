from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.when(F.col("event_type").isin("created", "assigned"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
    )
)

summarized_rows = (
    cleaned_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["ticket_id", "event_count"], ["ticket_id"], []],
        "ticket_id", "event_count",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("event_count").alias("total_event_count"),
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
    )
)
