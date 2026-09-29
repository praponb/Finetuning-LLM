from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    base.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

stats = (
    summarized_rows
    .groupBy("ticket_id")
    .agg(
        F.max("event_count").alias("max_event_count"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("created", "escalated", "assigned"), 1)) / F.count("*"), 1).alias("pct_event_type_in_created_escala"),
        F.round(F.stddev("event_count"), 2).alias("stddev_event_count"),
        F.max("event_at").alias("last_event_at"),
        F.sum(F.when(F.col("actor").isin("system", "customer"), F.col("event_count"))).alias("event_count_actor_in_system_customer"),
        F.sum("event_count").alias("total_event_count"),
    )
    .filter(F.col("total_event_count") > 3)
)

result = (
    stats
    .select(
        "ticket_id",
        "max_event_count",
        "stddev_event_count",
        "event_count_actor_in_system_customer",
        "total_event_count",
    )
    .orderBy(
        "event_count_actor_in_system_customer",
        F.col("total_event_count").desc(),
        "ticket_id",
    )
    .limit(10)
)
