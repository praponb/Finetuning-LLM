from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), F.col("id").desc())
w_ticket_id_event_type_or_unkn = Window.partitionBy("ticket_id", "event_type_or_unknown").orderBy(F.col("event_at").desc(), "id")
w_event_actor_count = Window.partitionBy("event_actor_count")

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("actor").isin("system", "customer"), 1)).alias("event_actor_count"),
    )
)

with_counts = (
    recent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_actor_count"), F.lit(0)).alias("event_actor_count"),
    )
)

unique_rows = (
    with_counts
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    unique_rows
    .select(
        "*",
        F.row_number().over(w_ticket_id_event_type_or_unkn).alias("event_at_seq"),
        F.max("event_actor_count").over(w_event_actor_count).alias("event_actor_count_max_event_actor_count"),
        F.ntile(3).over(w_ticket_id_event_type_or_unkn).alias("event_at_tercile"),
        F.round(F.avg("event_actor_count").over(w_event_actor_count), 2).alias("event_actor_count_avg_event_actor_count"),
        F.min("event_actor_count").over(w_event_actor_count).alias("event_actor_count_min_event_actor_count"),
    )
)

with_parent = (
    with_trends.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_clean",
        "s.event_actor_count_max_event_actor_count",
        "s.event_actor_count_min_event_actor_count",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("su.tier").alias("agent_tier"),
    )
)

joined = (
    with_parent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.csat").alias("ticket_csat_2"))
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.max("event_at").alias("last_event_at"),
        F.max("event_at").alias("last_event_at_2"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.last_event_at",
        "t.last_event_at_2",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_type_clean",
        "ticket_csat",
        "agent_tier",
        "ticket_csat_2",
        "first_event_at",
        "last_event_at_2",
        F.coalesce(F.col("ticket_csat_2"), F.lit(0)).alias("ticket_csat_2_filled"),
    )
)
