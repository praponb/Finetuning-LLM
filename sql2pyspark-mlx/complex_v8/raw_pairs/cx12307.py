from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter((F.col("event_type") != "resolved") | (F.col("actor").isin("agent", "customer")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

exploded_detail_events = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

annotated = (
    exploded_detail_events
    .select(
        "*",
        F.concat_ws("|", F.col("event_type"), F.col("event_actor")).alias("event_type_event_actor_label"),
        F.date_format(F.col("event_event_at"), "MMM yyyy").alias("event_event_at_label"),
    )
)

scored = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_id",
        "event_event_at",
        "event_actor",
        "event_type_event_actor_label",
        F.date_format(F.col("event_event_at"), "yyyy-MM").alias("event_event_at_label_2"),
        F.lower(F.regexp_replace(F.trim(F.col("event_actor")), " ", "_")).alias("event_actor_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_event_at")).alias("days_since_event_event_at"),
    )
)

grouped = (
    scored
    .groupBy("ticket_id")
    .agg(
        F.round(F.avg("days_since_event_event_at"), 2).alias("avg_days_since_event_event_at"),
        F.count("*").alias("cnt"),
        F.countDistinct("event_actor").alias("distinct_event_actors"),
        F.sum("days_since_event_event_at").alias("total_days_since_event_event_at"),
        F.sum(F.when(F.col("event_actor") == "agent", F.col("days_since_event_event_at"))).alias("days_since_event_event_at_event_actor_agent"),
        F.avg("days_since_event_event_at").alias("avg_days_since_event_event_at_check"),
    )
    .filter(F.col("avg_days_since_event_event_at_check") > 175)
    .drop("avg_days_since_event_event_at_check")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

profiled = (
    grouped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        "t.first_event_at",
        "t.first_event_at_2",
    )
)

result = (
    profiled
    .filter(F.col("first_event_at_2") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "ticket_id",
        "avg_days_since_event_event_at",
        "cnt",
        "distinct_event_actors",
        "total_days_since_event_event_at",
        "days_since_event_event_at_event_actor_agent",
        "first_event_at",
    )
    .orderBy("cnt", "distinct_event_actors")
)
