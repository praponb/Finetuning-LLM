from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type") == "resolved") & (F.col("ticket_id") >= 30))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
    )
)

ticket_id_stats = (
    scoped_events
    .groupBy("ticket_id")
    .agg(F.max("event_at_hour").alias("max_event_at_hour"))
)

vs_group = (
    scoped_events.alias("s")
    .join(ticket_id_stats.alias("g"), F.col("g.ticket_id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("s.event_at_hour") == F.col("g.max_event_at_hour"))
    .select(
        "s.*",
        "g.max_event_at_hour",
        F.round(F.col("s.event_at_hour") - F.col("g.max_event_at_hour"), 2).alias("event_at_hour_minus_max"),
    )
)

stats = (
    vs_group
    .filter(F.col("event_type") != "resolved")
    .groupBy("event_type")
    .agg(
        F.round(F.avg("event_at_hour"), 2).alias("avg_event_at_hour"),
        F.sum(F.when(F.col("actor") != "agent", F.col("event_at_hour_minus_max"))).alias("event_at_hour_minus_max_actor_agent"),
        F.round(F.sum("max_event_at_hour") / F.nullif(F.sum("event_at_hour"), F.lit(0)), 3).alias("max_event_at_hour_to_event_at_hour_ratio"),
        F.sum("event_at_hour").alias("total_event_at_hour"),
        F.sum("event_at_hour_minus_max").alias("sum_event_at_hour_minus_max_check"),
    )
    .filter(F.col("sum_event_at_hour_minus_max_check") > -9.6)
    .drop("sum_event_at_hour_minus_max_check")
)

screened = (
    stats
    .crossJoin(stats.agg(F.min("avg_event_at_hour").alias("overall_min_avg_event_at_hour")))
    .filter(F.col("avg_event_at_hour") > F.col("overall_min_avg_event_at_hour"))
    .drop("overall_min_avg_event_at_hour")
)

result = (
    screened
    .select(
        "event_type",
        "avg_event_at_hour",
        "event_at_hour_minus_max_actor_agent",
        "max_event_at_hour_to_event_at_hour_ratio",
        "total_event_at_hour",
    )
    .orderBy(F.col("event_type").desc())
)
