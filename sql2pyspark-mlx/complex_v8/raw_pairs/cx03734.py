from pyspark.sql import functions as F

recent_events = (
    df_ticket_events
    .filter((~F.col("event_type").isin("replied", "assigned")) & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

screened = (
    recent_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent") & (F.month(F.col("t.event_at")).isin(1, 3)), "left_semi")
    .filter(~F.col("actor").isin("customer", "system"))
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.max("event_at").alias("last_event_at"),
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    screened.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.last_event_at",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
    )
)

with_activity = (
    profiled
    .crossJoin(profiled.agg(F.avg("event_count").alias("overall_avg_event_count")))
    .filter(F.col("event_count") > F.col("overall_avg_event_count"))
    .drop("overall_avg_event_count")
)

result = (
    with_activity
    .groupingSets(
        [["ticket_id", "event_count"], ["ticket_id"], []],
        "ticket_id", "event_count",
    )
    .agg(F.round(F.avg("event_count"), 2).alias("avg_event_count"))
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_count").asc_nulls_last(),
    )
)
