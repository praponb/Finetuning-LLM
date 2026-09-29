from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter(F.col("actor") == "customer")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 1 DAYS")).alias("event_at_plus_1d"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
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

profiled = (
    recent_events.alias("s")
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
        F.count(F.when(F.col("event_type").isin("escalated", "reopened"), 1)).alias("event_event_type_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        "t.first_event_at",
    )
)

rollup_base = (
    summarized_rows
    .groupBy("actor", "ticket_id")
    .agg(
        F.max("event_at_plus_1d").alias("last_event_at_plus_1d"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.max("event_event_type_count").alias("max_event_event_type_count_check"),
    )
    .filter(F.col("max_event_event_type_count_check") > 3)
    .drop("max_event_event_type_count_check")
)

tagged = (
    rollup_base
    .filter(F.col("actor").isin("agent", "system"))
    .withColumn("days_since_last_event_at_plus_1d", F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_event_at_plus_1d")))
)

enriched = (
    tagged.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.closed_at") < F.lit("2025-02-15 09:00:00").cast("timestamp"))
    .select(
        "s.actor",
        "s.ticket_id",
        "s.last_event_at_plus_1d",
        "s.event_type_list",
        "s.days_since_last_event_at_plus_1d",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

result = (
    enriched
    .filter(F.col("ticket_id") <= 38)
    .select(
        "actor",
        "ticket_id",
        "days_since_last_event_at_plus_1d",
        "ticket_closed_at",
        F.date_format(F.col("ticket_closed_at"), "yyyy-MM-dd HH:00").alias("ticket_closed_at_label"),
    )
)
