from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_filled_event_event_type = Window.partitionBy("csat_filled", "event_event_type_count")

cleaned = (
    df_tickets
    .filter((F.col("requester_id").between(4, 18)) & (F.col("channel") != "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.csat_filled",
        "s.priority_slug",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("event_type") == "created", 1)).alias("event_event_type_count"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("event_actor_count"),
    )
)

with_stats = (
    joined.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_event_type_count"), F.lit(0)).alias("event_event_type_count"),
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

with_trends = (
    with_stats
    .select(
        "id",
        "requester_id",
        "closed_at",
        "priority_slug",
        "agent_team",
        "agent_hired_on",
        "event_event_type_count",
        "event_actor_count",
        F.min("event_count").over(w_csat_filled_event_event_type).alias("csat_filled_event_event_type_count_min_event_cou"),
        F.round(F.col("csat_filled") - F.avg("csat_filled").over(w_csat_filled_event_event_type), 2).alias("csat_filled_vs_csat_filled_event_event_type_coun"),
    )
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

profiled = (
    with_trends.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

without_matches = (
    profiled.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.hour(F.col("t.event_at")).between(9, 14)) & (~F.col("t.event_type").isin("resolved", "reopened")), "left_anti")
)

result = (
    without_matches
    .select("requester_id", "event_event_type_count")
    .distinct()
    .orderBy("event_event_type_count", F.col("requester_id").desc_nulls_first())
)
