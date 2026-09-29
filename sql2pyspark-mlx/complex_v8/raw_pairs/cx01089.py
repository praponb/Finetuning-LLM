from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor").orderBy("ticket_opened_at", "id")

recent = (
    df_ticket_events
    .filter((F.col("actor") == "customer") & (F.col("event_type") != "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 2 HOURS")).alias("event_at_plus_2h"),
    )
)

scored = (
    recent
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at_plus_2h")).alias("days_since_event_at_plus_2h"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    scored.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
        F.count(F.when(F.col("event_type").isin("resolved", "created"), 1)).alias("event_event_type_count"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
    )
)

without_matches = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("actor_len").alias("overall_min_actor_len")))
    .filter(F.col("actor_len") > F.col("overall_min_actor_len"))
    .drop("overall_min_actor_len")
)

with_details = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .filter(F.col("t.opened_at") < F.lit("2025-02-24 15:00:00").cast("timestamp"))
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.actor_len",
        "s.days_since_event_at_plus_2h",
        "s.event_count",
        "s.event_actor_count",
        "s.event_event_type_count",
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.opened_at").alias("ticket_opened_at"),
    )
)

latest = (
    with_details
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_count",
        "event_actor_count",
        "event_event_type_count",
        "ticket_agent_id",
        "ticket_opened_at",
    )
    .orderBy("event_type", "id")
    .limit(20)
)
