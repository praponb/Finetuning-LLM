from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
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
    scoped_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count_2"),
        F.min("event_at").alias("first_event_at"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
        "t.first_event_at",
    )
)

result = (
    with_counts
    .groupingSets(
        [["ticket_id", "event_type", "event_count_2"], ["ticket_id"], ["event_type"], []],
        "ticket_id", "event_type", "event_count_2",
    )
    .agg(
        F.grouping("ticket_id").alias("is_total_ticket_id"),
        F.round(F.sum("event_count") / F.nullif(F.sum("event_count_2"), F.lit(0)), 3).alias("event_count_to_event_count_2_ratio"),
        F.count("*").alias("cnt"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_count_2",
        "is_total_ticket_id",
        "event_count_to_event_count_2_ratio",
        "cnt",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("event_count_2").asc_nulls_last(),
    )
)
