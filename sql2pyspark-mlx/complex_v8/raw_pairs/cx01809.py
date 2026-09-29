from pyspark.sql import functions as F

in_window = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

annotated = (
    in_window
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        (F.floor(F.col("hours_since_open") / 10) * 10).alias("hours_since_open_bucket"),
        F.coalesce(F.col("hours_since_open"), F.lit(0.0)).alias("hours_since_open_filled"),
    )
)

annotated_2 = (
    annotated
    .withColumn("hours_since_open_bucket_distance", F.abs(F.col("hours_since_open_bucket") - 29))
)

result = (
    annotated_2
    .select(
        "id",
        "ticket_id",
        "priority",
        "agent_id",
        "priority_slug",
        "hours_since_open_filled",
        "hours_since_open_bucket_distance",
    )
    .orderBy("priority_slug", "id")
    .limit(5)
)
