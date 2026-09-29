from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.col("event_type").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
    )
)

derived = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_plus_7d",
        "event_at_plus_30m",
        F.to_date(F.col("event_at_plus_7d")).alias("event_at_plus_7d_date"),
        F.round((F.unix_timestamp(F.col("event_at_plus_7d")) - F.unix_timestamp(F.col("event_at_plus_30m"))) / 3600, 1).alias("hours_event_at_plus_30m_to_event_at_plus_7d"),
    )
)

enriched_ticket = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.hour(F.col("t.closed_at")).between(10, 13)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_at_plus_30m",
        "s.event_at_plus_7d_date",
        "s.hours_event_at_plus_30m_to_event_at_plus_7d",
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.requester_id").alias("ticket_requester_id"),
    )
)

scored = (
    enriched_ticket
    .filter(F.size(F.col("ticket_tags")) >= 2)
    .withColumn("actor_slug", F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")))
)

stats = (
    scored
    .groupBy(
        F.date_trunc("month", "event_at_plus_30m").alias("event_at_plus_30m_month"),
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum(F.when(F.col("actor").isin("customer", "system"), F.col("ticket_requester_id")).otherwise(0)).alias("ticket_requester_id_actor_in_customer_system"),
        F.countDistinct("actor").alias("distinct_actors"),
        F.max("hours_event_at_plus_30m_to_event_at_plus_7d").alias("max_hours_event_at_plus_30m_to_event_at_plus_7d_"),
    )
    .filter(F.col("max_hours_event_at_plus_30m_to_event_at_plus_7d_") > -1200.0)
    .drop("max_hours_event_at_plus_30m_to_event_at_plus_7d_")
)

annotated = (
    stats
    .filter(F.to_date(F.col("event_at_plus_30m_month")) < F.lit("2025-02-01").cast("date"))
    .select(
        "event_at_plus_30m_month",
        "actor_list",
        "ticket_requester_id_actor_in_customer_system",
        "distinct_actors",
        F.dayofweek(F.col("event_at_plus_30m_month")).alias("event_at_plus_30m_month_dayofweek"),
    )
)

result = (
    annotated
    .select(
        "event_at_plus_30m_month",
        "actor_list",
        "ticket_requester_id_actor_in_customer_system",
        "distinct_actors",
        (F.floor(F.col("distinct_actors") / 2) * 2).alias("distinct_actors_bucket"),
        (F.floor(F.col("distinct_actors") / 2) * 2).alias("distinct_actors_bucket_2"),
    )
    .orderBy(F.col("distinct_actors_bucket_2").desc(), "event_at_plus_30m_month")
    .limit(20)
)
