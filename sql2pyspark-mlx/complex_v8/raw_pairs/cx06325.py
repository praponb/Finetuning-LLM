from pyspark.sql import functions as F

covered = (
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

stats = (
    covered
    .groupBy("event_type", "ticket_id", "agent_id")
    .agg(
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    stats
    .filter(F.col("event_type").isin("reopened", "created"))
    .select(
        "event_type",
        "ticket_id",
        "agent_id",
        "distinct_prioritys",
        "cnt",
        (F.col("cnt") + F.col("distinct_prioritys")).alias("cnt_plus_distinct_prioritys"),
        F.abs(F.col("distinct_prioritys") - 4).alias("distinct_prioritys_distance"),
    )
    .orderBy(F.col("distinct_prioritys").desc(), F.col("event_type").desc())
)
