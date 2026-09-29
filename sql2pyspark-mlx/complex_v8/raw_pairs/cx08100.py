from pyspark.sql import functions as F

filtered = (
    df_ticket_events
    .filter(F.col("event_type").isin("replied", "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.event_at_plus_2h",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("su.tier").alias("agent_tier"),
    )
)

annotated = (
    combined_details
    .select(
        "*",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.hour(F.col("event_at")).alias("event_at_hour_2"),
        F.date_trunc("month", F.col("event_at_plus_2h")).alias("event_at_plus_2h_month"),
    )
)

annotated_2 = (
    annotated
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "ticket_opened_at",
        "agent_tier",
        "event_at_hour",
        "event_at_hour_2",
        (F.floor(F.col("event_at_hour_2") / 2) * 2).alias("event_at_hour_2_bucket"),
    )
)

enriched = (
    annotated_2
    .filter(F.col("event_at_hour_2_bucket") <= 14)
    .select("*", F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"))
)

per_group = (
    enriched
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_at_hour_2_bucket",
    )
    .agg(
        F.percentile_approx("event_at_hour_2", 0.5).alias("median_event_at_hour_2"),
        F.max("ticket_opened_at").alias("last_ticket_opened_at"),
        F.count(F.when(F.col("actor") == "customer", 1)).alias("n_actor_customer"),
        F.round(F.stddev("event_at_hour_2_bucket"), 2).alias("stddev_event_at_hour_2_bucket"),
        F.max("event_at_hour_2_bucket").alias("max_event_at_hour_2_bucket_check"),
    )
    .filter(F.col("max_event_at_hour_2_bucket_check") > 8)
    .drop("max_event_at_hour_2_bucket_check")
)

enriched_2 = (
    per_group
    .withColumn("median_event_at_hour_2_bucket", F.floor(F.col("median_event_at_hour_2") / 2) * 2)
)

result = (
    enriched_2
    .select(
        "event_at_month",
        "event_at_hour_2_bucket",
        "last_ticket_opened_at",
        "median_event_at_hour_2_bucket",
        F.coalesce(F.col("event_at_hour_2_bucket"), F.lit(0)).alias("event_at_hour_2_bucket_filled"),
    )
    .orderBy("last_ticket_opened_at", F.col("event_at_month").asc_nulls_last())
)
