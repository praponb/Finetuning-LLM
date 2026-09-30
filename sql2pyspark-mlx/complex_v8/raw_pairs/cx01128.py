from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
    )
)

event_type_stats = (
    prepared_events
    .groupBy("event_type")
    .agg(F.avg("event_at_hour").alias("avg_event_at_hour"))
)

compared = (
    prepared_events.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_at_hour") > F.col("g.avg_event_at_hour"))
    .select(
        "s.*",
        "g.avg_event_at_hour",
        F.round(F.col("s.event_at_hour") - F.col("g.avg_event_at_hour"), 2).alias("event_at_hour_minus_avg"),
    )
)

with_activity = (
    compared
    .crossJoin(compared.agg(F.avg("avg_event_at_hour").alias("overall_avg_avg_event_at_hour")))
    .filter(F.col("avg_event_at_hour") > F.col("overall_avg_avg_event_at_hour"))
    .drop("overall_avg_avg_event_at_hour")
)

exploded_detail = (
    with_activity.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.event_at_hour",
        "s.event_at_date",
        "s.event_at_day",
        "s.avg_event_at_hour",
        "s.event_at_hour_minus_avg",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

result = (
    exploded_detail
    .filter(F.col("event_at_date") >= F.lit("2024-06-01").cast("date"))
    .select(
        "id",
        "ticket_id",
        "actor",
        "event_at_hour",
        "event_at_date",
        "event_at_day",
        "avg_event_at_hour",
        "event_at_hour_minus_avg",
        "event_id",
        "event_actor",
        "event_event_at",
    )
    .orderBy(F.col("avg_event_at_hour").desc(), "id", "event_id")
    .limit(5)
)
