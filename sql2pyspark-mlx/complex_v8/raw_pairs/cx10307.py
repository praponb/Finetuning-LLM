from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 29)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
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

with_counts = (
    valid_events.alias("s")
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
        F.max("event_at").alias("last_event_at_2"),
        F.count("*").alias("event_count_2"),
    )
)

with_stats = (
    with_counts.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at_2",
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

with_details = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.channel").alias("ticket_channel"))
)

result = (
    with_details
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor_prefix",
        "event_count",
        "last_event_at_2",
        "event_count_2",
        "ticket_channel",
    )
    .orderBy(F.col("last_event_at_2").asc(), F.col("ticket_id").desc_nulls_first())
)
