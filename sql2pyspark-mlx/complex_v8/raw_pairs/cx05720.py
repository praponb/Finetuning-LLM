from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.concat_ws("|", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_details = (
    prepared_events.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.channel") != "web"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.actor_len",
        "s.actor_event_type_label",
        F.col("t.closed_at").alias("ticket_closed_at"),
    )
)

joined_ticket = (
    with_details.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.channel").alias("ticket_channel"),
    )
)

summary = (
    joined_ticket
    .groupBy(
        F.date_trunc("month", "ticket_opened_at").alias("ticket_opened_at_month"),
    )
    .agg(
        F.sum("actor_len").alias("total_actor_len"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("actor_len"), 2).alias("stddev_actor_len"),
        F.round(F.avg("actor_len"), 2).alias("avg_actor_len"),
        F.max("actor_len").alias("max_actor_len"),
    )
    .filter(F.col("total_actor_len") > 4)
)

with_activity = (
    summary
    .crossJoin(summary.agg(F.max("total_actor_len").alias("overall_max_total_actor_len")))
    .filter(F.col("total_actor_len") < F.col("overall_max_total_actor_len"))
    .drop("overall_max_total_actor_len")
)

result = (
    with_activity
    .select(
        "ticket_opened_at_month",
        "stddev_actor_len",
        "avg_actor_len",
        "max_actor_len",
        (F.col("stddev_actor_len") + F.col("total_actor_len")).alias("stddev_actor_len_plus_total_actor_len"),
    )
    .orderBy(F.col("avg_actor_len").asc())
)
